#include "nemo_iterator.h"

#include <iostream>
#include "nemo_set.h"
#include "nemo_hash.h"
#include "nemo_zset.h"
#include "xdebug.h"

nemo::Iterator::Iterator(rocksdb::Iterator *it,rocksdb::DBNemo * db_nemo, const IteratorOptions& iter_options)
  : it_(it),
    db_nemo_(db_nemo),
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

// Iterator endpoint: Right Open
nemo::IteratorRO::IteratorRO(rocksdb::Iterator *it,rocksdb::DBNemo * db_nemo, const IteratorOptions& iter_options)
  : it_(it),
    db_nemo_(db_nemo),
    ioptions_(iter_options) {
      Check();
    }

bool nemo::IteratorRO::Check() {
  valid_ = false;
  if (ioptions_.limit == 0 || !it_->Valid()) {
    // make next() safe to be called after previous return false.
    ioptions_.limit = 0;
    return false;
  } else {
    if (ioptions_.direction == kForward) {
      if (!ioptions_.end.empty() && it_->key().compare(ioptions_.end) >= 0) {
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

rocksdb::Slice nemo::IteratorRO::key() {
  return it_->key();
}

rocksdb::Slice nemo::IteratorRO::value() {
  return it_->value();
}

bool nemo::IteratorRO::Valid() {
  return valid_;
}

//  non-positive offset don't skip at all
void nemo::IteratorRO::Skip(int64_t offset) {
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

void nemo::IteratorRO::Next() {
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
nemo::KIterator::KIterator(rocksdb::Iterator *it,rocksdb::DBNemo * db_nemo, const IteratorOptions iter_options)
  : Iterator(it, db_nemo, iter_options) {
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

// KV KIteratorRO endpoint: Right Open
nemo::KIteratorRO::KIteratorRO(rocksdb::Iterator *it,rocksdb::DBNemo * db_nemo, const IteratorOptions iter_options)
  : IteratorRO(it, db_nemo, iter_options) {
  }

bool nemo::KIteratorRO::Valid() {
  return valid_;
}

void nemo::KIteratorRO::Next() {
  IteratorRO::Next();
}

void nemo::KIteratorRO::Skip(int64_t offset) {
  IteratorRO::Skip(offset);
}

// HASH
nemo::HIterator::HIterator(rocksdb::Iterator *it, rocksdb::DBNemo * db_nemo, const IteratorOptions iter_options, const rocksdb::Slice &key)
  : Iterator(it,db_nemo,iter_options) {
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
nemo::ZIterator::ZIterator(rocksdb::Iterator *it,rocksdb::DBNemo * db_nemo, const IteratorOptions iter_options, const rocksdb::Slice &key)
  : Iterator(it,db_nemo, iter_options) {
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
nemo::ZLexIterator::ZLexIterator(rocksdb::Iterator *it, rocksdb::DBNemo * db_nemo, const IteratorOptions iter_options, const rocksdb::Slice &key)
  : Iterator(it,db_nemo, iter_options) {
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
nemo::SIterator::SIterator(rocksdb::Iterator *it,rocksdb::DBNemo * db_nemo, const IteratorOptions iter_options, const rocksdb::Slice &key)
  : Iterator(it, db_nemo, iter_options) {
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
nemo::HmetaIterator::HmetaIterator(rocksdb::Iterator *it,rocksdb::DBNemo * db_nemo, const IteratorOptions iter_options, const rocksdb::Slice &key, bool skip_nil_index)
  : IteratorRO(it,db_nemo, iter_options), _skip_nil_index(skip_nil_index) {
    CheckAndLoadData();
  }

// check valid and load field_, value_
void nemo::HmetaIterator::CheckAndLoadData() {
  if (valid_) {
    rocksdb::Slice ks = IteratorRO::key();
    if (ks[0] != DataType::kHSize) {
      valid_ = false;
    }
  }
}

rocksdb::Slice nemo::HmetaIterator::rawkey(){ 
  return IteratorRO::key();
}

rocksdb::Slice nemo::HmetaIterator::key(){ 
  rocksdb::Slice s = IteratorRO::key(); 
  return rocksdb::Slice(s.data()+1,s.size()-1);
}

int64_t nemo::HmetaIterator::volume(){
  rocksdb::Slice value = IteratorRO::value();
  return *(int64_t *)(value.data()+ sizeof(int64_t));
}

rocksdb::Slice nemo::HmetaIterator::IndexInfo(){
  rocksdb::Slice value = IteratorRO::value();
  size_t len = sizeof(int64_t)*2;
  if(value.size()>len)
    return rocksdb::Slice(value.data()+len,value.size()-len);
  else
    return rocksdb::Slice();
}

bool nemo::HmetaIterator::Valid() {
  return valid_;
}

void nemo::HmetaIterator::Next() {
  IteratorRO::Next();
  CheckAndLoadData();
}

void nemo::HmetaIterator::Skip(int64_t offset) {
  IteratorRO::Skip(offset);
  CheckAndLoadData();
}

// List meta key
nemo::LmetaIterator::LmetaIterator(rocksdb::Iterator *it,rocksdb::DBNemo * db_nemo, const IteratorOptions iter_options, const rocksdb::Slice &key)
  : IteratorRO(it,db_nemo, iter_options) {    
    CheckAndLoadData();
  }

// check valid and load list meta data
void nemo::LmetaIterator::CheckAndLoadData() {
  if (valid_) {
    rocksdb::Slice ks = IteratorRO::key();
    if (ks[0] != DataType::kLMeta) {
      valid_ = false;
    }
  }
}

rocksdb::Slice nemo::LmetaIterator::rawkey(){ 
  return IteratorRO::key();
}

rocksdb::Slice nemo::LmetaIterator::key(){ 
  rocksdb::Slice s = IteratorRO::key(); 
  return rocksdb::Slice(s.data()+1,s.size()-1);
}

int64_t nemo::LmetaIterator::volume(){
  rocksdb::Slice value = IteratorRO::value();
  return *(int64_t *)(value.data()+ sizeof(int64_t));
}

bool nemo::LmetaIterator::Valid() {
  return valid_;
}

void nemo::LmetaIterator::Next() {
  IteratorRO::Next();
  CheckAndLoadData();
}

void nemo::LmetaIterator::Skip(int64_t offset) {
  IteratorRO::Skip(offset);
  CheckAndLoadData();
}

// Set meta key
nemo::SmetaIterator::SmetaIterator(rocksdb::Iterator *it, rocksdb::DBNemo * db_nemo, const IteratorOptions iter_options, const rocksdb::Slice &key)
  : IteratorRO(it,db_nemo, iter_options) {  
    CheckAndLoadData();
  }

// check valid and load Set meta data
void nemo::SmetaIterator::CheckAndLoadData() {
  if (valid_) {
    rocksdb::Slice ks = IteratorRO::key();

    if (ks[0] != DataType::kSSize) {
      valid_ = false;
    }
  }
}

rocksdb::Slice nemo::SmetaIterator::rawkey(){ 
  return IteratorRO::key();
}

rocksdb::Slice nemo::SmetaIterator::key(){ 
  rocksdb::Slice s = IteratorRO::key(); 
  return rocksdb::Slice(s.data()+1,s.size()-1);
}

int64_t nemo::SmetaIterator::volume(){
  rocksdb::Slice value = IteratorRO::value();
  return *(int64_t *)(value.data()+ sizeof(int64_t));
}

bool nemo::SmetaIterator::Valid() {
  return valid_;
}

void nemo::SmetaIterator::Next() {
  IteratorRO::Next();
  CheckAndLoadData();
}

void nemo::SmetaIterator::Skip(int64_t offset) {
  IteratorRO::Skip(offset);
  CheckAndLoadData();
}

// ZSet meta key
nemo::ZmetaIterator::ZmetaIterator(rocksdb::Iterator *it,rocksdb::DBNemo * db_nemo, const IteratorOptions iter_options, const rocksdb::Slice &key)
  : IteratorRO(it,db_nemo, iter_options) {     
    CheckAndLoadData();
  }

// check valid and load ZSet meta data
void nemo::ZmetaIterator::CheckAndLoadData() {
  if (valid_) {
    rocksdb::Slice ks = IteratorRO::key();

    if (ks[0] != DataType::kZSize) {
      valid_ = false;
    }
  }
}

rocksdb::Slice nemo::ZmetaIterator::rawkey(){ 
  return IteratorRO::key();
}

rocksdb::Slice nemo::ZmetaIterator::key(){ 
  rocksdb::Slice s = IteratorRO::key(); 
  return rocksdb::Slice(s.data()+1,s.size()-1);
}

int64_t nemo::ZmetaIterator::volume(){
  rocksdb::Slice value = IteratorRO::value();
  return *(int64_t *)(value.data()+ sizeof(int64_t));
}

bool nemo::ZmetaIterator::Valid() {
  return valid_;
}

void nemo::ZmetaIterator::Next() {
  IteratorRO::Next();
  CheckAndLoadData();
}

void nemo::ZmetaIterator::Skip(int64_t offset) {
  IteratorRO::Skip(offset);
  CheckAndLoadData();
}