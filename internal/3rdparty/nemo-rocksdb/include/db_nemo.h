//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
#pragma once
#ifndef ROCKSDB_LITE

#include "rocksdb/utilities/stackable_db.h"
#include "rocksdb/db.h"

namespace rocksdb {

struct KVOT {
  public:
  Slice key;
  Slice val;
  int8_t  ops;
  int32_t ttl;
  KVOT(){}
  KVOT(std::string & k, std::string & v, int o, int t) : key(k), val(v), ops(o), ttl(t) {}
  KVOT(const char * k, const char * v, int o, int t) : key(k), val(v), ops(o), ttl(t) {}
};

class DBNemo: public StackableDB {
 public:

  static Status Open(Options& options, const std::string& dbname,
                     DBNemo** dbptr,
                     char meta_prefix = '\0', bool read_only = false);

  static Status Open(DBOptions& db_options, const std::string& dbname,
                     const std::vector<ColumnFamilyDescriptor>& column_families,
                     std::vector<ColumnFamilyHandle*>* handles,
                     DBNemo** dbptr,
                     char meta_prefix = '\0',
                     bool read_only = false);

  using StackableDB::Put;
  virtual Status Put(const WriteOptions& options, const Slice& key, const Slice& val, int32_t ttl) {
    return Put(options, db_->DefaultColumnFamily(), key, val, ttl);
  };
  virtual Status Put(const WriteOptions& options, ColumnFamilyHandle* column_family, const Slice& key, const Slice& val, int32_t ttl) = 0;

  using StackableDB::Write;
  virtual Status Write(const WriteOptions& opts, WriteBatch* updates, int32_t ttl) = 0;
  virtual Status WriteBatchTtl(const WriteOptions& opts, std::vector<KVOT>& kvots) = 0;

  virtual Status PutWithExpiredTime(const WriteOptions& options, const Slice& key, const Slice& val, int32_t expired_time) = 0;
  virtual Status WriteWithExpiredTime(const WriteOptions& opts, WriteBatch* updates, int32_t expired_time) = 0;
  virtual Status PutWithKeyVersion(const WriteOptions& options, const Slice& key, const Slice& val) = 0;
  virtual Status WriteWithKeyVersion(const WriteOptions& opts, WriteBatch* updates) = 0;
  virtual Status WriteWithOldKeyTTL(const WriteOptions& opts, WriteBatch* updates) = 0;
  virtual Status GetKeyTTL(const ReadOptions& options, const Slice& key, int32_t *ttl) = 0;
  virtual void StopAllBackgroundWork(bool wait) = 0;

 protected:
  explicit DBNemo(DB* db) : StackableDB(db) {}
};

}  // namespace rocksdb
#endif  // ROCKSDB_LITE
