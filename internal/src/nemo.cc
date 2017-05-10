#include "nemo.h"

#include <dirent.h>
#include <iostream>
#include <algorithm>
#include <sys/stat.h>
#include <sys/types.h>
#include "nemo_list.h"
#include "nemo_zset.h"
#include "nemo_set.h"
#include "nemo_hash.h"
#include "port.h"
#include "util.h"
#include "xdebug.h"

namespace nemo {

Nemo::Nemo(const std::string &db_path, const Options &options)
    : db_path_(db_path),
    save_flag_(false),
    bgtask_flag_(true),
    bg_cv_(&mutex_bgtask_),
    scan_keynum_exit_(false),
    dump_to_terminate_(false) {

   pthread_mutex_init(&(mutex_cursors_), NULL);
   pthread_mutex_init(&(mutex_dump_), NULL);
   pthread_mutex_init(&(mutex_spop_counts_), NULL);
   if (db_path_[db_path_.length() - 1] != '/') {
     db_path_.append("/");
   }

   mkpath(db_path_.c_str(), 0755);
   mkpath((db_path_ + "kv").c_str(), 0755);
   mkpath((db_path_ + "hash").c_str(), 0755);
   mkpath((db_path_ + "list").c_str(), 0755);
   mkpath((db_path_ + "zset").c_str(), 0755);
   mkpath((db_path_ + "set").c_str(), 0755);

   cursors_store_.cur_size_ = 0;
   cursors_store_.max_size_ = 5000;
   cursors_store_.list_.clear();
   cursors_store_.map_.clear();

   spop_counts_store_.cur_size_ = 0;
   spop_counts_store_.max_size_ = 100;
   spop_counts_store_.list_.clear();
   spop_counts_store_.map_.clear();

   // Open Options
   open_options_.create_if_missing = true;
   open_options_.write_buffer_size = options.write_buffer_size;
   open_options_.max_manifest_file_size = 64*1024*1024;
   if (!options.compression) {
     open_options_.compression = rocksdb::CompressionType::kNoCompression;
   }
   if (options.max_open_files > 0) {
     open_options_.max_open_files = options.max_open_files;
   }

   if (options.target_file_size_base > 0) {
     open_options_.target_file_size_base = (uint64_t)options.target_file_size_base;
   }
   if (options.target_file_size_multiplier > 0) {
     open_options_.target_file_size_multiplier = options.target_file_size_multiplier;
   }

   if (options.max_background_flushes > 0 && options.max_background_flushes <= 4) {
      open_options_.max_background_flushes = options.max_background_flushes;
   }
   if (options.max_background_compactions > 0 && options.max_background_compactions <= 4) {
      open_options_.max_background_compactions = options.max_background_compactions;
   }

   //open_options_.max_bytes_for_level_base = (128 << 20);

   rocksdb::DBWithTTL *db_ttl;

// kv with multi cf
/*
   open_options_.meta_prefix = rocksdb::kMetaPrefix_KV;
   std::vector<rocksdb::ColumnFamilyDescriptor> column_families;
   column_families.push_back(
        rocksdb::ColumnFamilyDescriptor(rocksdb::kDefaultColumnFamilyName, rocksdb::ColumnFamilyOptions())
   );

   column_families.push_back(
      rocksdb::ColumnFamilyDescriptor(
          "raft_meta", rocksdb::ColumnFamilyOptions())
   );

   column_families.push_back(
      rocksdb::ColumnFamilyDescriptor(
          "raft_log", rocksdb::ColumnFamilyOptions())
   );
   std::vector<int32_t> ttls(3);
   ttls[0] = 0;
   ttls[1] = 0;
   ttls[2] = 0;  

   rocksdb::Status s = rocksdb::DBWithTTL::Open(open_options_, db_path_ + "kv",
                                              column_families, &cf_handle_, &db_ttl,ttls);
   if (!s.ok()) {
     fprintf (stderr, "[FATAL] open kv db failed, %s\n", s.ToString().c_str());
     exit(-1);
   }



   kv_db_ = std::unique_ptr<rocksdb::DBWithTTL>(db_ttl);
*/


   open_options_.meta_prefix = rocksdb::kMetaPrefix_KV;
   rocksdb::Status s = rocksdb::DBWithTTL::Open(open_options_, db_path_ + "kv", &db_ttl);
   if (!s.ok()) {
     fprintf (stderr, "[FATAL] open kv db failed, %s\n", s.ToString().c_str());
     exit(-1);
   }
   kv_db_ = std::unique_ptr<rocksdb::DBWithTTL>(db_ttl);

   open_options_.meta_prefix = rocksdb::kMetaPrefix_HASH;
   s = rocksdb::DBWithTTL::Open(open_options_, db_path_ + "hash", &db_ttl);
   if (!s.ok()) {
     fprintf (stderr, "[FATAL] open hash db failed, %s\n", s.ToString().c_str());
     exit(-1);
   }
   hash_db_ = std::unique_ptr<rocksdb::DBWithTTL>(db_ttl);

   open_options_.meta_prefix = rocksdb::kMetaPrefix_LIST;
   s = rocksdb::DBWithTTL::Open(open_options_, db_path_ + "list", &db_ttl);
   if (!s.ok()) {
     fprintf (stderr, "[FATAL] open list db failed, %s\n", s.ToString().c_str());
     exit(-1);
   }
   list_db_ = std::unique_ptr<rocksdb::DBWithTTL>(db_ttl);

   open_options_.meta_prefix = rocksdb::kMetaPrefix_ZSET;
   s = rocksdb::DBWithTTL::Open(open_options_, db_path_ + "zset", &db_ttl);
   if (!s.ok()) {
     fprintf (stderr, "[FATAL] open zset db failed, %s\n", s.ToString().c_str());
     exit(-1);
   }
   zset_db_ = std::unique_ptr<rocksdb::DBWithTTL>(db_ttl);

   open_options_.meta_prefix = rocksdb::kMetaPrefix_SET;
   s = rocksdb::DBWithTTL::Open(open_options_, db_path_ + "set", &db_ttl);
   if (!s.ok()) {
     fprintf (stderr, "[FATAL] open set db failed, %s\n", s.ToString().c_str());
     exit(-1);
   }
   set_db_ = std::unique_ptr<rocksdb::DBWithTTL>(db_ttl);

   open_options_.meta_prefix = rocksdb::kMetaPrefix_META;
   s = rocksdb::DBWithTTL::Open(open_options_, db_path_ + "meta", &db_ttl);
   if (!s.ok()) {
     fprintf (stderr, "[FATAL] open meta db failed, %s\n", s.ToString().c_str());
     exit(-1);
   }
   meta_db_ = std::unique_ptr<rocksdb::DBWithTTL>(db_ttl);

   open_options_.meta_prefix = rocksdb::kMetaPrefix_RAFT;
   s = rocksdb::DBWithTTL::Open(open_options_, db_path_ + "raft", &db_ttl);
   if (!s.ok()) {
     fprintf (stderr, "[FATAL] open raft db failed, %s\n", s.ToString().c_str());
     exit(-1);
   }
   raft_db_ = std::unique_ptr<rocksdb::DBWithTTL>(db_ttl);

   // Add separator of Meta and data
   hash_db_->Put(rocksdb::WriteOptions(), "h", "");
   list_db_->Put(rocksdb::WriteOptions(), "l", "");
   zset_db_->Put(rocksdb::WriteOptions(), "y", "");
   zset_db_->Put(rocksdb::WriteOptions(), "z", "");
   set_db_->Put(rocksdb::WriteOptions(), "s", "");

   // Start BGThread
   s = StartBGThread();
   if (!s.ok()) {
     log_err("start bg thread error: %s", s.ToString().c_str());
     fprintf (stderr, "[FATAL] start bg thread failed, %s\n", s.ToString().c_str());
     exit(-1);
   }
};

/*
rocksdb::ColumnFamilyHandle* Nemo::GetCFHandleByname(const std::string name){
    if(name == "raft_meta"){
      return cf_handle_[1];
    }
    else if(name == "raft_log"){
      return cf_handle_[2];
    }
};

rocksdb::Status Nemo::CFWrite(rocksdb::WriteBatch *wb){
  return kv_db_->WriteWithKeyTTL(rocksdb::WriteOptions(),wb,0);
};

rocksdb::Status Nemo::CFGet(rocksdb::ColumnFamilyHandle* cf_h,const std::string & key,std::string * value){
  return kv_db_->Get(rocksdb::ReadOptions(),cf_h,key,value);
};
*/

}   // namespace nemo
