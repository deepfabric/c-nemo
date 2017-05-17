#include "nemo_iterator.h"

#include <iostream>
#include "nemo_set.h"
#include "nemo_hash.h"
#include "nemo_zset.h"
#include "xdebug.h"

nemo::Iterator::Iterator(rocksdb::Iterator *it, const IteratorOptions& iter_options)
  : it_(it),
    ioptions_(iter_options) {
      Check();
    }

bool nemo::Iterator::Check() {
  valid_ = false;
  if (ioptions_.limit == 0 || !it_->Valid()) {
    // make next() safe to be called after previous return false.
    ioptions_.limit = 0;
    return false;
  } else {
    if (ioptions_.direction == kForward) {
      if (!ioptions_.end.empty() && it_->key().compare(ioptions_.end) > 0) {
        ioptions_.limit = 0;
        return false;
      }
    } else {
      if(!ioptions_.end.empty() && it_->key().compare(ioptions_.end) < 0) {
        ioptions_.limit = 0;
        return false;
      }
    }
    ioptions_.limit --;
    valid_ = true;
    return true;
  }
}

rocksdb::Slice nemo::Iterator::key() {
  return it_->key();
}

rocksdb::Slice nemo::Iterator::value() {
  return it_->value();
}

bool nemo::Iterator::Valid() {
  return valid_;
}

//  non-positive offset don't skip at all
void nemo::Iterator::Skip(int64_t offset) {
  if (offset > 0) {
    while (offset-- > 0) {
      if (ioptions_.direction == kForward){
        it_->Next();
      } else {
        it_->Prev();
      }

      if (!Check()) {
        return;
      }
    }
  }
}

void nemo::Iterator::Next() {
  if (valid_) {
    if (ioptions_.direction == kForward){
      it_->Next();
    } else {
      it_->Prev();
    }
    
    Check();
  }
}

// KV
nemo::KIterator::KIterator(rocksdb::Iterator *it, const IteratorOptions iter_options)
  : Iterator(it, iter_options) {
  CheckAndLoadData();
  }

void nemo::KIterator::CheckAndLoadData() {
  if (valid_) {
    rocksdb::Slice ks = Iterator::key();
    rocksdb::Slice vs = Iterator::value();
    this->key_.assign(ks.data(), ks.size());
    this->value_.assign(vs.data(), vs.size());
  }
}

bool nemo::KIterator::Valid() {
  return valid_;
}

void nemo::KIterator::Next() {
  Iterator::Next();
  CheckAndLoadData();
}

void nemo::KIterator::Skip(int64_t offset) {
  Iterator::Skip(offset);
  CheckAndLoadData();
}

// HASH
nemo::HIterator::HIterator(rocksdb::Iterator *it, const IteratorOptions iter_options, const rocksdb::Slice &key)
  : Iterator(it, iter_options) {
    this->key_.assign(key.data(), key.size());
    CheckAndLoadData();
  }

// check valid and load field_, value_
void nemo::HIterator::CheckAndLoadData() {
  if (valid_) {
    rocksdb::Slice ks = Iterator::key();

    if (ks[0] == DataType::kHash) {
      std::string k;
      if (DecodeHashKey(ks, &k, &this->field_) != -1) {
        if (k == this->key_) {
          rocksdb::Slice vs = Iterator::value();
          this->value_.assign(vs.data(), vs.size());
          return ;
        }
      }
    }
  }
  valid_ = false;
}

bool nemo::HIterator::Valid() {
  return valid_;
}

void nemo::HIterator::Next() {
  Iterator::Next();
  CheckAndLoadData();
}

void nemo::HIterator::Skip(int64_t offset) {
  Iterator::Skip(offset);
  CheckAndLoadData();
}

// ZSET
nemo::ZIterator::ZIterator(rocksdb::Iterator *it, const IteratorOptions iter_options, const rocksdb::Slice &key)
  : Iterator(it, iter_options) {
    this->key_.assign(key.data(), key.size());
    CheckAndLoadData();
  }

// check valid and assign member_ and score_
void nemo::ZIterator::CheckAndLoadData() {
  if (valid_) {
    rocksdb::Slice ks = Iterator::key();
    if (ks[0] == DataType::kZScore) {
      std::string k;
      if (DecodeZScoreKey(ks, &k, &this->member_, &this->score_) != -1) {
        if (k == this->key_) {
          return ;
        }
      }
    }
  }
  valid_ = false;
}

bool nemo::ZIterator::Valid() {
  return valid_;
}

void nemo::ZIterator::Next() {
  Iterator::Next();
  CheckAndLoadData();
}

void nemo::ZIterator::Skip(int64_t offset) {
  Iterator::Skip(offset);
  CheckAndLoadData();
}

// ZLexIterator
nemo::ZLexIterator::ZLexIterator(rocksdb::Iterator *it, const IteratorOptions iter_options, const rocksdb::Slice &key)
  : Iterator(it, iter_options) {
    this->key_.assign(key.data(), key.size());
    CheckAndLoadData();
  }

void nemo::ZLexIterator::CheckAndLoadData() {
  if (valid_) {
    rocksdb::Slice ks = Iterator::key();

    if (ks[0] == DataType::kZSet) {
      std::string k;
      if (DecodeZSetKey(ks, &k, &this->member_) != -1) {
        if (k == this->key_) {
          return ;
        }
      }
    }
  }
  valid_ = false;
}

bool nemo::ZLexIterator::Valid() {
  return valid_;
}

void nemo::ZLexIterator::Next() {
  Iterator::Next();
  CheckAndLoadData();
}

void nemo::ZLexIterator::Skip(int64_t offset) {
  Iterator::Skip(offset);
  CheckAndLoadData();
}

// SET
nemo::SIterator::SIterator(rocksdb::Iterator *it, const IteratorOptions iter_options, const rocksdb::Slice &key)
  : Iterator(it, iter_options) {
    this->key_.assign(key.data(), key.size());
    CheckAndLoadData();
  }

// check valid and assign member_
void nemo::SIterator::CheckAndLoadData() {
  if (valid_) {
    rocksdb::Slice ks = Iterator::key();

    if (ks[0] == DataType::kSet) {
      std::string k;
      if (DecodeSetKey(ks, &k, &this->member_) != -1) {
        if (k == this->key_) {
          return ;
        }
      }
    }
  }
  valid_ = false;
}

bool nemo::SIterator::Valid() {
  return valid_;
}

void nemo::SIterator::Next() {
  Iterator::Next();
  CheckAndLoadData();
}

void nemo::SIterator::Skip(int64_t offset) {
  Iterator::Skip(offset);
  CheckAndLoadData();
}

// HASH meta key
nemo::HmetaIterator::HmetaIterator(rocksdb::Iterator *it, const IteratorOptions iter_options, const rocksdb::Slice &key)
  : Iterator(it, iter_options) {
    this->key_.assign(key.data(), key.size());      
    CheckAndLoadData();
  }

// check valid and load field_, value_
void nemo::HmetaIterator::CheckAndLoadData() {
  if (valid_) {
    rocksdb::Slice ks = Iterator::key();

    if (ks[0] == DataType::kHSize) {
      this->key_.assign(ks.data()+1,ks.size()-1);
      std::string val(Iterator::value().data(),Iterator::value().size());
      meta_.DecodeFrom(val);
      return ;
    }
  }
  valid_ = false;
}

bool nemo::HmetaIterator::Valid() {
  return valid_;
}

void nemo::HmetaIterator::Next() {
  Iterator::Next();
  CheckAndLoadData();
}

void nemo::HmetaIterator::Skip(int64_t offset) {
  Iterator::Skip(offset);
  CheckAndLoadData();
}

// List meta key
nemo::LmetaIterator::LmetaIterator(rocksdb::Iterator *it, const IteratorOptions iter_options, const rocksdb::Slice &key)
  : Iterator(it, iter_options) {
    this->key_.assign(key.data(), key.size());      
    CheckAndLoadData();
  }

// check valid and load list meta data
void nemo::LmetaIterator::CheckAndLoadData() {
  if (valid_) {
    rocksdb::Slice ks = Iterator::key();

    if (ks[0] == DataType::kLMeta) {
      this->key_.assign(ks.data()+1,ks.size()-1);
      std::string val(Iterator::value().data(),Iterator::value().size());
      meta_.DecodeFrom(val);
      return ;
    }
  }
  valid_ = false;
}

bool nemo::LmetaIterator::Valid() {
  return valid_;
}

void nemo::LmetaIterator::Next() {
  Iterator::Next();
  CheckAndLoadData();
}

void nemo::LmetaIterator::Skip(int64_t offset) {
  Iterator::Skip(offset);
  CheckAndLoadData();
}

// Set meta key
nemo::SmetaIterator::SmetaIterator(rocksdb::Iterator *it, const IteratorOptions iter_options, const rocksdb::Slice &key)
  : Iterator(it, iter_options) {
    this->key_.assign(key.data(), key.size());      
    CheckAndLoadData();
  }

// check valid and load Set meta data
void nemo::SmetaIterator::CheckAndLoadData() {
  if (valid_) {
    rocksdb::Slice ks = Iterator::key();

    if (ks[0] == DataType::kSSize) {
      this->key_.assign(ks.data()+1,ks.size()-1);
      std::string val(Iterator::value().data(),Iterator::value().size());
      meta_.DecodeFrom(val);
      return ;
    }
  }
  valid_ = false;
}

bool nemo::SmetaIterator::Valid() {
  return valid_;
}

void nemo::SmetaIterator::Next() {
  Iterator::Next();
  CheckAndLoadData();
}

void nemo::SmetaIterator::Skip(int64_t offset) {
  Iterator::Skip(offset);
  CheckAndLoadData();
}

// ZSet meta key
nemo::ZmetaIterator::ZmetaIterator(rocksdb::Iterator *it, const IteratorOptions iter_options, const rocksdb::Slice &key)
  : Iterator(it, iter_options) {
    this->key_.assign(key.data(), key.size());      
    CheckAndLoadData();
  }

// check valid and load ZSet meta data
void nemo::ZmetaIterator::CheckAndLoadData() {
  if (valid_) {
    rocksdb::Slice ks = Iterator::key();

    if (ks[0] == DataType::kZSize) {
      this->key_.assign(ks.data()+1,ks.size()-1);
      std::string val(Iterator::value().data(),Iterator::value().size());
      meta_.DecodeFrom(val);
      return ;
    }
  }
  valid_ = false;
}

bool nemo::ZmetaIterator::Valid() {
  return valid_;
}

void nemo::ZmetaIterator::Next() {
  Iterator::Next();
  CheckAndLoadData();
}

void nemo::ZmetaIterator::Skip(int64_t offset) {
  Iterator::Skip(offset);
  CheckAndLoadData();
}