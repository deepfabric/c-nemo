// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once

#ifndef ROCKSDB_LITE
#include <deque>
#include <string>
#include <vector>

#include "db/db_impl.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/options.h"
#include "rocksdb/compaction_filter.h"
#include "rocksdb/merge_operator.h"
#include "rocksdb/utilities/utility_db.h"
#include "rocksdb/utilities/db_ttl.h"

namespace rocksdb {

class DBWithTTLImpl : public DBWithTTL {
 public:
  static void SanitizeOptions(int32_t ttl, ColumnFamilyOptions* options,
                              Env* env);

  explicit DBWithTTLImpl(DB* db);
  explicit DBWithTTLImpl(DB* db, int ttl);

  virtual ~DBWithTTLImpl();

  Status CreateColumnFamilyWithTtl(const ColumnFamilyOptions& options,
                                   const std::string& column_family_name,
                                   ColumnFamilyHandle** handle,
                                   int ttl) override;

  Status CreateColumnFamily(const ColumnFamilyOptions& options,
                            const std::string& column_family_name,
                            ColumnFamilyHandle** handle) override;

  using StackableDB::Put;
  virtual Status Put(const WriteOptions& options,
                     ColumnFamilyHandle* column_family, const Slice& key,
                     const Slice& val) override;

  using StackableDB::Get;
  virtual Status Get(const ReadOptions& options,
                     ColumnFamilyHandle* column_family, const Slice& key,
                     std::string* value) override;

  using StackableDB::MultiGet;
  virtual std::vector<Status> MultiGet(
      const ReadOptions& options,
      const std::vector<ColumnFamilyHandle*>& column_family,
      const std::vector<Slice>& keys,
      std::vector<std::string>* values) override;

  using StackableDB::KeyMayExist;
  virtual bool KeyMayExist(const ReadOptions& options,
                           ColumnFamilyHandle* column_family, const Slice& key,
                           std::string* value,
                           bool* value_found = nullptr) override;

  using StackableDB::Merge;
  virtual Status Merge(const WriteOptions& options,
                       ColumnFamilyHandle* column_family, const Slice& key,
                       const Slice& value) override;

  virtual Status Write(const WriteOptions& opts, WriteBatch* updates) override;

  using StackableDB::NewIterator;
  virtual Iterator* NewIterator(const ReadOptions& opts,
                                ColumnFamilyHandle* column_family) override;

  virtual DB* GetBaseDB() override { return db_; }


  static bool IsStale(const Slice& value, int32_t ttl, Env* env);

  static Status AppendTS(const Slice& val, std::string* val_with_ts, Env* env);

  static Status SanityCheckTimestamp(const Slice& str);

  static Status StripTS(std::string* str);

  static Status StripVersionAndTS(std::string* str);


  static const int32_t kMinTimestamp = 1368146402;  // 05/09/2013:5:40PM GMT-8

  static const int32_t kMaxTimestamp = 2147483647;  // 01/18/2038:7:14PM GMT-8

  // add Key TTL and Key version feature
  static int32_t GetTTLFromNow(const Slice& value, int32_t ttl, Env* env);
  static Status AppendTSWithKeyTTL(const Slice& val, std::string* val_with_ts, Env* env, int32_t ttl);
  static Status AppendTSWithExpiredTime(const Slice& val, std::string* val_with_ts, Env* env, int32_t expired_time);
  int32_t ttl_;

  //Status SanityCheckVersionAndTimestamp(const Slice &key, const Slice& value);

  //int32_t GetKeyVersion(ColumnFamilyHandle* column_family, const Slice& key);
  static Status AppendVersionAndExpiredTime(const Slice& val, std::string* val_with_ver_ts, Env* env, int32_t version, int32_t expired_time);
  static Status AppendVersionAndKeyTTL(const Slice& val, std::string* val_with_ver_ts, Env* env, int32_t version, int32_t ttl);
};

class TtlIterator : public Iterator {

 public:
  explicit TtlIterator(Iterator* iter, DB* db) : iter_(iter), db_(db) { assert(iter_); }

  ~TtlIterator() { delete iter_; }

  bool Valid() const override {
    //bool ret = iter_->Valid();
    //if (ret) {
    if (iter_->Valid()) {
      // check key version
      if (db_->meta_prefix_ != kMetaPrefix_KV) {
        int32_t fresh_version = db_->GetKeyVersion(iter_->key());
        Slice val = iter_->value();
        int32_t key_version = DecodeFixed32(val.data() + val.size() - DBImpl::kVersionLength - DBImpl::kTSLength);
        if (key_version >= fresh_version) {
          return true;
        }
      } else {
        return true;
      }
    }
    return false;
  }

  void SeekToFirst() override { iter_->SeekToFirst(); }

  void SeekToLast() override { iter_->SeekToLast(); }

  void Seek(const Slice& target) override { iter_->Seek(target); }

  void Next() override { iter_->Next(); }

  void Prev() override { iter_->Prev(); }

  Slice key() const override { return iter_->key(); }

  int32_t timestamp() const {
    return DecodeFixed32(iter_->value().data() + iter_->value().size() -
                         DBImpl::kTSLength);
  }

  Slice value() const override {
    // TODO: handle timestamp corruption like in general iterator semantics
    assert(DBWithTTLImpl::SanityCheckTimestamp(iter_->value()).ok());
    Slice trimmed_value = iter_->value();
    trimmed_value.size_ -= DBImpl::kVersionLength + DBImpl::kTSLength;
    return trimmed_value;
  }

  Status status() const override { return iter_->status(); }

 private:
  Iterator* iter_;
  DB* db_;
};

class TtlCompactionFilter : public CompactionFilter {
 public:
  TtlCompactionFilter(
      int32_t ttl, Env* env, const CompactionFilter* user_comp_filter,
      std::unique_ptr<const CompactionFilter> user_comp_filter_from_factory =
          nullptr)
      : ttl_(ttl),
        env_(env),
        user_comp_filter_(user_comp_filter),
        user_comp_filter_from_factory_(
            std::move(user_comp_filter_from_factory)) {
    // Unlike the merge operator, compaction filter is necessary for TTL, hence
    // this would be called even if user doesn't specify any compaction-filter
    if (!user_comp_filter_) {
      user_comp_filter_ = user_comp_filter_from_factory_.get();
    }
  }

  virtual bool Filter(int level, const Slice& key, const Slice& old_val,
                      std::string* new_val, bool* value_changed) const override;
 // {

 //   if (db_->meta_prefix_ == kMetaPrefix_KV) {
 //       if (DBWithTTLImpl::IsStale(old_val, 0, env_)) {
 //         return true;
 //       }
 //   } else {
 //     // reserve meta key for hash, list, zset, set
 //     if ((key.data())[0] == db_->meta_prefix_) {
 //       return false;
 //     }

 //     int32_t fresh_version = 0;
 //     std::string value;

 //     // Get meta key and value
 //     std::string meta_key(1, db_->meta_prefix_);
 //     int32_t len = *((uint8_t *)key.data() + 1);
 //     meta_key.append(key.data() + 2, len);

 //     Status st = db_->Get(ReadOptions(), DefaultColumnFamily(), meta_key, &value);

 //     if (st.ok()) {
 //       if (DBWithTTLImpl::IsStale(value, 0, env_)) {
 //         return true;
 //       }

 //       // check key version
 //       fresh_version = DecodeFixed32(value.data() + value.size() - DBImpl::kVersionLength - DBImpl::kTSLength);

 //       //int32_t fresh_version = db_->GetKeyVersion(key);
 //       int32_t key_version = DecodeFixed32(old_val.data() + old_val.size() - DBImpl::kVersionLength - DBImpl::kTSLength);
 //       if (key_version < fresh_version) {
 //         return true;
 //       }
 //     }
 //   }

 //   if (user_comp_filter_ == nullptr) {
 //     return false;
 //   }
 //   assert(old_val.size() >= DBImpl::kTSLength);
 //   Slice old_val_without_ts(old_val.data(),
 //                            old_val.size() - DBImpl::kTSLength);
 //   if (user_comp_filter_->Filter(level, key, old_val_without_ts, new_val,
 //                                 value_changed)) {
 //     return true;
 //   }
 //   if (*value_changed) {
 //     new_val->append(
 //         old_val.data() + old_val.size() - DBImpl::kTSLength,
 //         DBImpl::kTSLength);
 //   }
 //   return false;
 // }

  virtual const char* Name() const override { return "Delete By TTL"; }

  //virtual void SetDB(DBImpl *db) { db_ = db; }
 private:
  int32_t ttl_;
  Env* env_;
  const CompactionFilter* user_comp_filter_;
  std::unique_ptr<const CompactionFilter> user_comp_filter_from_factory_;
};

class TtlCompactionFilterFactory : public CompactionFilterFactory {
 public:
  TtlCompactionFilterFactory(
      int32_t ttl, Env* env,
      std::shared_ptr<CompactionFilterFactory> comp_filter_factory)
      : ttl_(ttl), env_(env), user_comp_filter_factory_(comp_filter_factory) {}

  virtual std::unique_ptr<CompactionFilter> CreateCompactionFilter(
      const CompactionFilter::Context& context) override {
    return std::unique_ptr<TtlCompactionFilter>(new TtlCompactionFilter(
        ttl_, env_, nullptr,
        std::move(user_comp_filter_factory_->CreateCompactionFilter(context))));
  }

  virtual const char* Name() const override {
    return "TtlCompactionFilterFactory";
  }

 private:
  int32_t ttl_;
  Env* env_;
  std::shared_ptr<CompactionFilterFactory> user_comp_filter_factory_;
};

class TtlMergeOperator : public MergeOperator {

 public:
  explicit TtlMergeOperator(const std::shared_ptr<MergeOperator>& merge_op,
                            Env* env)
      : user_merge_op_(merge_op), env_(env) {
    assert(merge_op);
    assert(env);
  }

  virtual bool FullMerge(const Slice& key, const Slice* existing_value,
                         const std::deque<std::string>& operands,
                         std::string* new_value, Logger* logger) const
      override {
    const uint32_t ts_len = DBWithTTLImpl::kTSLength;
    if (existing_value && existing_value->size() < ts_len) {
      Log(InfoLogLevel::ERROR_LEVEL, logger,
          "Error: Could not remove timestamp from existing value.");
      return false;
    }

    // Extract time-stamp from each operand to be passed to user_merge_op_
    std::deque<std::string> operands_without_ts;
    for (const auto& operand : operands) {
      if (operand.size() < ts_len) {
        Log(InfoLogLevel::ERROR_LEVEL, logger,
            "Error: Could not remove timestamp from operand value.");
        return false;
      }
      operands_without_ts.push_back(operand.substr(0, operand.size() - ts_len));
    }

    // Apply the user merge operator (store result in *new_value)
    bool good = true;
    if (existing_value) {
      Slice existing_value_without_ts(existing_value->data(),
                                      existing_value->size() - ts_len);
      good = user_merge_op_->FullMerge(key, &existing_value_without_ts,
                                       operands_without_ts, new_value, logger);
    } else {
      good = user_merge_op_->FullMerge(key, nullptr, operands_without_ts,
                                       new_value, logger);
    }

    // Return false if the user merge operator returned false
    if (!good) {
      return false;
    }

    // Augment the *new_value with the ttl time-stamp
    int64_t curtime;
    if (!env_->GetCurrentTime(&curtime).ok()) {
      Log(InfoLogLevel::ERROR_LEVEL, logger,
          "Error: Could not get current time to be attached internally "
          "to the new value.");
      return false;
    } else {
      char ts_string[ts_len];
      EncodeFixed32(ts_string, (int32_t)curtime);
      new_value->append(ts_string, ts_len);
      return true;
    }
  }

  virtual bool PartialMergeMulti(const Slice& key,
                                 const std::deque<Slice>& operand_list,
                                 std::string* new_value, Logger* logger) const
      override {
    const uint32_t ts_len = DBWithTTLImpl::kTSLength;
    std::deque<Slice> operands_without_ts;

    for (const auto& operand : operand_list) {
      if (operand.size() < ts_len) {
        Log(InfoLogLevel::ERROR_LEVEL, logger,
            "Error: Could not remove timestamp from value.");
        return false;
      }

      operands_without_ts.push_back(
          Slice(operand.data(), operand.size() - ts_len));
    }

    // Apply the user partial-merge operator (store result in *new_value)
    assert(new_value);
    if (!user_merge_op_->PartialMergeMulti(key, operands_without_ts, new_value,
                                           logger)) {
      return false;
    }

    // Augment the *new_value with the ttl time-stamp
    int64_t curtime;
    if (!env_->GetCurrentTime(&curtime).ok()) {
      Log(InfoLogLevel::ERROR_LEVEL, logger,
          "Error: Could not get current time to be attached internally "
          "to the new value.");
      return false;
    } else {
      char ts_string[ts_len];
      EncodeFixed32(ts_string, (int32_t)curtime);
      new_value->append(ts_string, ts_len);
      return true;
    }
  }

  virtual const char* Name() const override { return "Merge By TTL"; }

 private:
  std::shared_ptr<MergeOperator> user_merge_op_;
  Env* env_;
};
}
#endif  // ROCKSDB_LITE
