//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include "rocksdb/utilities/write_batch_with_index.h"

#include <memory>

#include "rocksdb/comparator.h"
#include "rocksdb/iterator.h"
#include "db/column_family.h"
#include "db/merge_context.h"
#include "db/merge_helper.h"
#include "db/skiplist.h"
#include "util/arena.h"
#include "utilities/write_batch_with_index/write_batch_with_index_internal.h"

namespace rocksdb {

// when direction == forward
// * current_at_base_ <=> base_iterator > delta_iterator
// when direction == backwards
// * current_at_base_ <=> base_iterator < delta_iterator
// always:
// * equal_keys_ <=> base_iterator == delta_iterator
class BaseDeltaIterator : public Iterator {
 public:
  BaseDeltaIterator(Iterator* base_iterator, WBWIIterator* delta_iterator,
                    const Comparator* comparator)
      : forward_(true),
        current_at_base_(true),
        equal_keys_(false),
        status_(Status::OK()),
        base_iterator_(base_iterator),
        delta_iterator_(delta_iterator),
        comparator_(comparator) {}

  virtual ~BaseDeltaIterator() {}

  bool Valid() const override {
    return current_at_base_ ? BaseValid() : DeltaValid();
  }

  void SeekToFirst() override {
    forward_ = true;
    base_iterator_->SeekToFirst();
    delta_iterator_->SeekToFirst();
    UpdateCurrent();
  }

  void SeekToLast() override {
    forward_ = false;
    base_iterator_->SeekToLast();
    delta_iterator_->SeekToLast();
    UpdateCurrent();
  }

  void Seek(const Slice& k) override {
    forward_ = true;
    base_iterator_->Seek(k);
    delta_iterator_->Seek(k);
    UpdateCurrent();
  }

  void Next() override {
    if (!Valid()) {
      status_ = Status::NotSupported("Next() on invalid iterator");
    }

    if (!forward_) {
      // Need to change direction
      // if our direction was backward and we're not equal, we have two states:
      // * both iterators are valid: we're already in a good state (current
      // shows to smaller)
      // * only one iterator is valid: we need to advance that iterator
      forward_ = true;
      equal_keys_ = false;
      if (!BaseValid()) {
        assert(DeltaValid());
        base_iterator_->SeekToFirst();
      } else if (!DeltaValid()) {
        delta_iterator_->SeekToFirst();
      } else if (current_at_base_) {
        // Change delta from larger than base to smaller
        AdvanceDelta();
      } else {
        // Change base from larger than delta to smaller
        AdvanceBase();
      }
      if (DeltaValid() && BaseValid()) {
        if (Compare() == 0) {
          equal_keys_ = true;
        }
      }
    }
    Advance();
  }

  void Prev() override {
    if (!Valid()) {
      status_ = Status::NotSupported("Prev() on invalid iterator");
    }

    if (forward_) {
      // Need to change direction
      // if our direction was backward and we're not equal, we have two states:
      // * both iterators are valid: we're already in a good state (current
      // shows to smaller)
      // * only one iterator is valid: we need to advance that iterator
      forward_ = false;
      equal_keys_ = false;
      if (!BaseValid()) {
        assert(DeltaValid());
        base_iterator_->SeekToLast();
      } else if (!DeltaValid()) {
        delta_iterator_->SeekToLast();
      } else if (current_at_base_) {
        // Change delta from less advanced than base to more advanced
        AdvanceDelta();
      } else {
        // Change base from less advanced than delta to more advanced
        AdvanceBase();
      }
      if (DeltaValid() && BaseValid()) {
        if (Compare() == 0) {
          equal_keys_ = true;
        }
      }
    }

    Advance();
  }

  Slice key() const override {
    return current_at_base_ ? base_iterator_->key()
                            : delta_iterator_->Entry().key;
  }

  Slice value() const override {
    return current_at_base_ ? base_iterator_->value()
                            : delta_iterator_->Entry().value;
  }

  Status status() const override {
    if (!status_.ok()) {
      return status_;
    }
    if (!base_iterator_->status().ok()) {
      return base_iterator_->status();
    }
    return delta_iterator_->status();
  }

 private:
  // -1 -- delta less advanced than base
  // 0 -- delta == base
  // 1 -- delta more advanced than base
  int Compare() const {
    assert(delta_iterator_->Valid() && base_iterator_->Valid());
    int cmp = comparator_->Compare(delta_iterator_->Entry().key,
                                   base_iterator_->key());
    if (forward_) {
      return cmp;
    } else {
      return -cmp;
    }
  }
  bool IsDeltaDelete() {
    assert(DeltaValid());
    return delta_iterator_->Entry().type == kDeleteRecord;
  }
  void AssertInvariants() {
#ifndef NDEBUG
    if (!Valid()) {
      return;
    }
    if (!BaseValid()) {
      assert(!current_at_base_ && delta_iterator_->Valid());
      return;
    }
    if (!DeltaValid()) {
      assert(current_at_base_ && base_iterator_->Valid());
      return;
    }
    // we don't support those yet
    assert(delta_iterator_->Entry().type != kMergeRecord &&
           delta_iterator_->Entry().type != kLogDataRecord);
    int compare = comparator_->Compare(delta_iterator_->Entry().key,
                                       base_iterator_->key());
    if (forward_) {
      // current_at_base -> compare < 0
      assert(!current_at_base_ || compare < 0);
      // !current_at_base -> compare <= 0
      assert(current_at_base_ && compare >= 0);
    } else {
      // current_at_base -> compare > 0
      assert(!current_at_base_ || compare > 0);
      // !current_at_base -> compare <= 0
      assert(current_at_base_ && compare <= 0);
    }
    // equal_keys_ <=> compare == 0
    assert((equal_keys_ || compare != 0) && (!equal_keys_ || compare == 0));
#endif
  }

  void Advance() {
    if (equal_keys_) {
      assert(BaseValid() && DeltaValid());
      AdvanceBase();
      AdvanceDelta();
    } else {
      if (current_at_base_) {
        assert(BaseValid());
        AdvanceBase();
      } else {
        assert(DeltaValid());
        AdvanceDelta();
      }
    }
    UpdateCurrent();
  }

  void AdvanceDelta() {
    if (forward_) {
      delta_iterator_->Next();
    } else {
      delta_iterator_->Prev();
    }
  }
  void AdvanceBase() {
    if (forward_) {
      base_iterator_->Next();
    } else {
      base_iterator_->Prev();
    }
  }
  bool BaseValid() const { return base_iterator_->Valid(); }
  bool DeltaValid() const { return delta_iterator_->Valid(); }
  void UpdateCurrent() {
    while (true) {
      equal_keys_ = false;
      if (!BaseValid()) {
        // Base has finished.
        if (!DeltaValid()) {
          // Finished
          return;
        }
        if (IsDeltaDelete()) {
          AdvanceDelta();
        } else {
          current_at_base_ = false;
          return;
        }
      } else if (!DeltaValid()) {
        // Delta has finished.
        current_at_base_ = true;
        return;
      } else {
        int compare = Compare();
        if (compare <= 0) {  // delta bigger or equal
          if (compare == 0) {
            equal_keys_ = true;
          }
          if (!IsDeltaDelete()) {
            current_at_base_ = false;
            return;
          }
          // Delta is less advanced and is delete.
          AdvanceDelta();
          if (equal_keys_) {
            AdvanceBase();
          }
        } else {
          current_at_base_ = true;
          return;
        }
      }
    }

    AssertInvariants();
  }

  bool forward_;
  bool current_at_base_;
  bool equal_keys_;
  Status status_;
  std::unique_ptr<Iterator> base_iterator_;
  std::unique_ptr<WBWIIterator> delta_iterator_;
  const Comparator* comparator_;  // not owned
};

typedef SkipList<WriteBatchIndexEntry*, const WriteBatchEntryComparator&>
    WriteBatchEntrySkipList;

class WBWIIteratorImpl : public WBWIIterator {
 public:
  WBWIIteratorImpl(uint32_t column_family_id,
                   WriteBatchEntrySkipList* skip_list,
                   const ReadableWriteBatch* write_batch)
      : column_family_id_(column_family_id),
        skip_list_iter_(skip_list),
        write_batch_(write_batch),
        valid_(false) {}

  virtual ~WBWIIteratorImpl() {}

  virtual bool Valid() const override { return valid_; }

  virtual void SeekToFirst() override {
    valid_ = true;
    WriteBatchIndexEntry search_entry(WriteBatchIndexEntry::kFlagMin,
                                      column_family_id_);
    skip_list_iter_.Seek(&search_entry);
    ReadEntry();
  }

  virtual void SeekToLast() override {
    valid_ = true;
    WriteBatchIndexEntry search_entry(WriteBatchIndexEntry::kFlagMin,
                                      column_family_id_ + 1);
    skip_list_iter_.Seek(&search_entry);
    if (!skip_list_iter_.Valid()) {
      skip_list_iter_.SeekToLast();
    } else {
      skip_list_iter_.Prev();
    }
    ReadEntry();
  }

  virtual void Seek(const Slice& key) override {
    valid_ = true;
    WriteBatchIndexEntry search_entry(&key, column_family_id_);
    skip_list_iter_.Seek(&search_entry);
    ReadEntry();
  }

  virtual void Next() override {
    skip_list_iter_.Next();
    ReadEntry();
  }

  virtual void Prev() override {
    skip_list_iter_.Prev();
    ReadEntry();
  }

  virtual const WriteEntry& Entry() const override { return current_; }

  virtual Status status() const override { return status_; }

  const WriteBatchIndexEntry* GetRawEntry() const {
    return skip_list_iter_.key();
  }

 private:
  uint32_t column_family_id_;
  WriteBatchEntrySkipList::Iterator skip_list_iter_;
  const ReadableWriteBatch* write_batch_;
  Status status_;
  bool valid_;
  WriteEntry current_;

  void ReadEntry() {
    if (!status_.ok() || !skip_list_iter_.Valid()) {
      valid_ = false;
      return;
    }
    const WriteBatchIndexEntry* iter_entry = skip_list_iter_.key();
    if (iter_entry == nullptr ||
        iter_entry->column_family != column_family_id_) {
      valid_ = false;
      return;
    }
    Slice blob;
    status_ = write_batch_->GetEntryFromDataOffset(
        iter_entry->offset, &current_.type, &current_.key, &current_.value,
        &blob);
    if (!status_.ok()) {
      valid_ = false;
    } else if (current_.type != kPutRecord && current_.type != kDeleteRecord &&
               current_.type != kMergeRecord) {
      valid_ = false;
      status_ = Status::Corruption("write batch index is corrupted");
    }
  }
};

struct WriteBatchWithIndex::Rep {
  Rep(const Comparator* index_comparator, size_t reserved_bytes = 0,
      bool _overwrite_key = false)
      : write_batch(reserved_bytes),
        comparator(index_comparator, &write_batch),
        skip_list(comparator, &arena),
        overwrite_key(_overwrite_key),
        last_entry_offset(0) {}
  ReadableWriteBatch write_batch;
  WriteBatchEntryComparator comparator;
  Arena arena;
  WriteBatchEntrySkipList skip_list;
  bool overwrite_key;
  size_t last_entry_offset;

  // Remember current offset of internal write batch, which is used as
  // the starting offset of the next record.
  void SetLastEntryOffset() { last_entry_offset = write_batch.GetDataSize(); }

  // In overwrite mode, find the existing entry for the same key and update it
  // to point to the current entry.
  // Return true if the key is found and updated.
  bool UpdateExistingEntry(ColumnFamilyHandle* column_family, const Slice& key);
  bool UpdateExistingEntryWithCfId(uint32_t column_family_id, const Slice& key);

  // Add the recent entry to the update.
  // In overwrite mode, if key already exists in the index, update it.
  void AddOrUpdateIndex(ColumnFamilyHandle* column_family, const Slice& key);
  void AddOrUpdateIndex(const Slice& key);

  // Allocate an index entry pointing to the last entry in the write batch and
  // put it to skip list.
  void AddNewEntry(uint32_t column_family_id);

  // Clear all updates buffered in this batch.
  void Clear();
};

bool WriteBatchWithIndex::Rep::UpdateExistingEntry(
    ColumnFamilyHandle* column_family, const Slice& key) {
  uint32_t cf_id = GetColumnFamilyID(column_family);
  return UpdateExistingEntryWithCfId(cf_id, key);
}

bool WriteBatchWithIndex::Rep::UpdateExistingEntryWithCfId(
    uint32_t column_family_id, const Slice& key) {
  if (!overwrite_key) {
    return false;
  }

  WBWIIteratorImpl iter(column_family_id, &skip_list, &write_batch);
  iter.Seek(key);
  if (!iter.Valid()) {
    return false;
  }
  if (comparator.CompareKey(column_family_id, key, iter.Entry().key) != 0) {
    return false;
  }
  WriteBatchIndexEntry* non_const_entry =
      const_cast<WriteBatchIndexEntry*>(iter.GetRawEntry());
  non_const_entry->offset = last_entry_offset;
  return true;
}

void WriteBatchWithIndex::Rep::AddOrUpdateIndex(
    ColumnFamilyHandle* column_family, const Slice& key) {
  if (!UpdateExistingEntry(column_family, key)) {
    uint32_t cf_id = GetColumnFamilyID(column_family);
    const auto* cf_cmp = GetColumnFamilyUserComparator(column_family);
    if (cf_cmp != nullptr) {
      comparator.SetComparatorForCF(cf_id, cf_cmp);
    }
    AddNewEntry(cf_id);
  }
}

void WriteBatchWithIndex::Rep::AddOrUpdateIndex(const Slice& key) {
  if (!UpdateExistingEntryWithCfId(0, key)) {
    AddNewEntry(0);
  }
}

void WriteBatchWithIndex::Rep::AddNewEntry(uint32_t column_family_id) {
    auto* mem = arena.Allocate(sizeof(WriteBatchIndexEntry));
    auto* index_entry =
        new (mem) WriteBatchIndexEntry(last_entry_offset, column_family_id);
    skip_list.Insert(index_entry);
  }

  void WriteBatchWithIndex::Rep::Clear() {
    write_batch.Clear();
    arena.~Arena();
    new (&arena) Arena();
    skip_list.~WriteBatchEntrySkipList();
    new (&skip_list) WriteBatchEntrySkipList(comparator, &arena);
    last_entry_offset = 0;
  }


WriteBatchWithIndex::WriteBatchWithIndex(
    const Comparator* default_index_comparator, size_t reserved_bytes,
    bool overwrite_key)
    : rep(new Rep(default_index_comparator, reserved_bytes, overwrite_key)) {}

WriteBatchWithIndex::~WriteBatchWithIndex() { delete rep; }

WriteBatch* WriteBatchWithIndex::GetWriteBatch() { return &rep->write_batch; }

WBWIIterator* WriteBatchWithIndex::NewIterator() {
  return new WBWIIteratorImpl(0, &(rep->skip_list), &rep->write_batch);
}

WBWIIterator* WriteBatchWithIndex::NewIterator(
    ColumnFamilyHandle* column_family) {
  return new WBWIIteratorImpl(GetColumnFamilyID(column_family),
                              &(rep->skip_list), &rep->write_batch);
}

Iterator* WriteBatchWithIndex::NewIteratorWithBase(
    ColumnFamilyHandle* column_family, Iterator* base_iterator) {
  if (rep->overwrite_key == false) {
    assert(false);
    return nullptr;
  }
  return new BaseDeltaIterator(base_iterator, NewIterator(column_family),
                               GetColumnFamilyUserComparator(column_family));
}

Iterator* WriteBatchWithIndex::NewIteratorWithBase(Iterator* base_iterator) {
  if (rep->overwrite_key == false) {
    assert(false);
    return nullptr;
  }
  // default column family's comparator
  return new BaseDeltaIterator(base_iterator, NewIterator(),
                               rep->comparator.default_comparator());
}

void WriteBatchWithIndex::Put(ColumnFamilyHandle* column_family,
                              const Slice& key, const Slice& value) {
  rep->SetLastEntryOffset();
  rep->write_batch.Put(column_family, key, value);
  rep->AddOrUpdateIndex(column_family, key);
}

void WriteBatchWithIndex::Put(const Slice& key, const Slice& value) {
  rep->SetLastEntryOffset();
  rep->write_batch.Put(key, value);
  rep->AddOrUpdateIndex(key);
}

void WriteBatchWithIndex::Merge(ColumnFamilyHandle* column_family,
                                const Slice& key, const Slice& value) {
  rep->SetLastEntryOffset();
  rep->write_batch.Merge(column_family, key, value);
  rep->AddOrUpdateIndex(column_family, key);
}

void WriteBatchWithIndex::Merge(const Slice& key, const Slice& value) {
  rep->SetLastEntryOffset();
  rep->write_batch.Merge(key, value);
  rep->AddOrUpdateIndex(key);
}

void WriteBatchWithIndex::PutLogData(const Slice& blob) {
  rep->write_batch.PutLogData(blob);
}

void WriteBatchWithIndex::Delete(ColumnFamilyHandle* column_family,
                                 const Slice& key) {
  rep->SetLastEntryOffset();
  rep->write_batch.Delete(column_family, key);
  rep->AddOrUpdateIndex(column_family, key);
}

void WriteBatchWithIndex::Delete(const Slice& key) {
  rep->SetLastEntryOffset();
  rep->write_batch.Delete(key);
  rep->AddOrUpdateIndex(key);
}

void WriteBatchWithIndex::Clear() { rep->Clear(); }

Status WriteBatchWithIndex::GetFromBatch(ColumnFamilyHandle* column_family,
                                         const DBOptions& options,
                                         const Slice& key, std::string* value) {
  Status s;
  MergeContext merge_context;

  WriteBatchWithIndexInternal::Result result =
      WriteBatchWithIndexInternal::GetFromBatch(options, this, column_family,
                                                key, &merge_context,
                                                &rep->comparator, value, &s);

  switch (result) {
    case WriteBatchWithIndexInternal::Result::kFound:
    case WriteBatchWithIndexInternal::Result::kError:
      return s;
    case WriteBatchWithIndexInternal::Result::kDeleted:
    case WriteBatchWithIndexInternal::Result::kNotFound:
      return Status::NotFound();
    case WriteBatchWithIndexInternal::Result::kMergeInProgress:
      return Status::MergeInProgress("");
    default:
      assert(false);
  }

  return s;
}

Status WriteBatchWithIndex::GetFromBatchAndDB(DB* db,
                                              const ReadOptions& read_options,
                                              const Slice& key,
                                              std::string* value) {
  return GetFromBatchAndDB(db, read_options, db->DefaultColumnFamily(), key,
                           value);
}

Status WriteBatchWithIndex::GetFromBatchAndDB(DB* db,
                                              const ReadOptions& read_options,
                                              ColumnFamilyHandle* column_family,
                                              const Slice& key,
                                              std::string* value) {
  Status s;
  MergeContext merge_context;
  const DBOptions& options = db->GetDBOptions();

  std::string batch_value;
  WriteBatchWithIndexInternal::Result result =
      WriteBatchWithIndexInternal::GetFromBatch(
          options, this, column_family, key, &merge_context, &rep->comparator,
          &batch_value, &s);

  if (result == WriteBatchWithIndexInternal::Result::kFound) {
    value->assign(batch_value.data(), batch_value.size());
    return s;
  }
  if (result == WriteBatchWithIndexInternal::Result::kDeleted) {
    return Status::NotFound();
  }
  if (result == WriteBatchWithIndexInternal::Result::kError) {
    return s;
  }
  assert(result == WriteBatchWithIndexInternal::Result::kMergeInProgress ||
         result == WriteBatchWithIndexInternal::Result::kNotFound);

  // Did not find key in batch OR could not resolve Merges.  Try DB.
  s = db->Get(read_options, column_family, key, value);

  if (s.ok() || s.IsNotFound()) {  // DB Get Suceeded
    if (result == WriteBatchWithIndexInternal::Result::kMergeInProgress) {
      // Merge result from DB with merges in Batch
      auto cfh = reinterpret_cast<ColumnFamilyHandleImpl*>(column_family);
      const MergeOperator* merge_operator =
          cfh->cfd()->ioptions()->merge_operator;
      Statistics* statistics = options.statistics.get();
      Env* env = options.env;
      Logger* logger = options.info_log.get();

      Slice db_slice(*value);
      Slice* merge_data;
      if (s.ok()) {
        merge_data = &db_slice;
      } else {  // Key not present in db (s.IsNotFound())
        merge_data = nullptr;
      }

      s = MergeHelper::TimedFullMerge(
          key, merge_data, merge_context.GetOperands(), merge_operator,
          statistics, env, logger, value);
    }
  }

  return s;
}

}  // namespace rocksdb
