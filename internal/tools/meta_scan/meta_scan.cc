#include <iostream>
#include <assert.h>
#include "xdebug.h"
#include <string>
#include "nemo.h"

using namespace nemo;

void Usage() {
  std::cout << "Usage: " << std::endl;
  std::cout << "./meta_scan db_path type pattern start_key end_key" << std::endl;
  std::cout << "type is one of: kv, hash, list, zset, set, all" << std::endl;
  std::cout << "Example: " << std::endl;
  std::cout << "./meta_scan ./db list k1 k2\\*" << std::endl;
}

void PrintMetaSpecify(nemo::Nemo *const db, DBType type, 
    const std::string& type_name, const std::string& pattern, const std::string& start_key, const std::string& end_key) {
  // Scan metas info
  std::map<std::string, MetaPtr> metas;
  int64_t sum_vol = 0;
  Status s = db->ScanMetasSpecify(type, pattern, metas);
  if (!s.ok()) {
    log_err("Failed to scan metas");
    return;
  }
  // Print
  std::cout << "---------------- Begin Scan[" << type_name << "] ----------------" << std::endl;
  std::map<std::string, MetaPtr>::iterator it = metas.begin();
  for (; it != metas.end(); ++it) {
    if(it->first >= start_key && it->first <= end_key){
    	std::cout << "Key[" << it->first 
      		<< "], Metas[" << it->second->ToString() << "]" << std::endl;
        sum_vol += it->second->Volume();
    }
  }
  std::cout << "total volume : " << sum_vol << std::endl;
  std::cout << "---------------- End Scan[" << type_name << "] ----------------" << std::endl;
}

void PrintMeta(nemo::Nemo *const db, const std::string& type, 
    const std::string& pattern, const std::string& start_key, const std::string& end_key) {
  bool all = false;
  if (type == "all") {
    all = true;
  }
  if (all || type == "hash") {
    PrintMetaSpecify(db, kHASH_DB, "hash", pattern, start_key, end_key);
  } 
  if (all || type == "list") {
    PrintMetaSpecify(db, kLIST_DB, "list", pattern, start_key, end_key);
  } 
  if (all || type == "set") {
    PrintMetaSpecify(db, kSET_DB, "set", pattern, start_key, end_key);
  } 
  if (all || type == "zset") {
    PrintMetaSpecify(db, kZSET_DB, "zset", pattern, start_key, end_key);
  }
}


int main(int argc, char **argv)
{
  if (argc != 6) {
    Usage();
    log_err("not enough parameter");
  }
  std::string path(argv[1]);
  std::string db_type(argv[2]);
  std::string pattern(argv[3]);
  std::string start_key(argv[4]);
  std::string end_key(argv[5]);
  if (db_type != "hash" && db_type != "list"
      && db_type != "set" && db_type != "zset"
      && db_type != "all") {
    Usage();
    log_err("invalid type parameter");
  }
  // Create nemo handle
  nemo::Options option;
  option.write_buffer_size = 268435456;
  option.target_file_size_base = 20971520;
  log_info("Prepare DB...");
  nemo::Nemo* db = new nemo::Nemo(path, option);
  assert(db);
  log_info("SCan Begin");

  PrintMeta(db, db_type, pattern, start_key, end_key);
  delete db;

  log_info("SCan Finshed");
  return 0;
}
