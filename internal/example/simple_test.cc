#include <iostream>
#include <vector>
#include <string>
#include <ctime>
#include <inttypes.h>
#include <limits>
#include <unistd.h>

#include "nemo.h"
#include "xdebug.h"

#include "nemo_volume_iterator.h"

using namespace nemo;

Nemo *n;

int main() {
  nemo::Options options;
  options.target_file_size_base = 20 * 1024 * 1024;

  n = new Nemo("/tmp/nemo_simple_test", options); 
  std::string res;
  Status s ;
  int HSetRes;
  //test for HMDel
  s = n->HSet("HMDel", "field1", "val1", &HSetRes);
  assert(s.ok());
  s = n->HSet("HMDel", "field2", "val2", &HSetRes);
  assert(s.ok()); 
  std::vector<std::string> fields(0);   
  fields.push_back("field1");
  fields.push_back("field2");
  fields.push_back("field3");
  int64_t HMDel_res = 0;
  s = n->HMDel("HMDel",fields,&HMDel_res);
  assert(s.ok());
  assert(HMDel_res = 2);
  std::cout <<"HMDel OK"<< std::endl;

  s = n->HSet("Key", "field1", "val1", &HSetRes);
  assert(s.ok());
  s = n->HSet("Key", "field2", "val2", &HSetRes);
  assert(s.ok());  
  int64_t l ;
  s = n->HLen("Key", &l);
  assert(s.ok());   
  std::cout << "HLen return: " << l << std::endl;

  rocksdb::DBNemo * meta = n->GetMetaHandle();

  std::string nKey,nVal,startkey;
  startkey = "";
  s = n->PutWithHandle(meta,"AFirstKey","FirstVal",false);
  assert(s.ok());
  s = n->SeekWithHandle(meta,startkey,&nKey,&nVal);
  assert(s.ok());
  assert(nKey == "AFirstKey");
  assert(nVal == "FirstVal");

  s = n->PutWithHandle(meta,"Hello","World",false);
  assert(s.ok());
  s = n->GetWithHandle(meta,"Hello",&res);
  assert(s.ok());
  assert(res == "World");
  rocksdb::WriteBatch wb;
  wb.Put("MetaKey1","MetaVal1");
  wb.Put("MetaKey2","MetaVal2");
  s = n->BatchWrite(meta,&wb,false);
  assert(s.ok());
  s = n->GetWithHandle(meta,"MetaKey2",&res);
  assert(s.ok());  
  assert(res == "MetaVal2"); 

  s = n->Set("kvdb","test",0);
  assert(s.ok());
  rocksdb::DBNemo * kvdb = n->GetKvHandle();
  rocksdb::WriteBatch wb2;
  wb2.Put("Hello","World");
  wb2.Delete("kvdb");
  s = n->BatchWrite(kvdb,&wb2,true);
  assert(s.ok());
  s = n->Get("Hello",&res);
  assert(s.ok());  
  assert(res == "World");

  std::cout << "scanwithhandle: A-MetaKey2" << std::endl;
  KIteratorRO* kit = n->KScanWithHandle(meta,"A","MetaKey2",100,true);
  while(kit->Valid()){
    std::string key,val;
    key.assign(kit->key().data(),kit->key().size());
    val.assign(kit->value().data(),kit->value().size());
    std::cout << "key:" << key << std::endl
              << "val:" << val << std::endl;
    kit->Next();
  }
  delete kit;

  s = n->RangeDelWithHandle(meta,"A","MetaKey2",100);
  assert(s.ok());  
  s = n->GetWithHandle(meta,"Hello",&res);
  assert(s.IsNotFound());

  std::cout << "scanwithhandle again: A-z" << std::endl;
  kit = n->KScanWithHandle(meta,"A","z",100,true);
  while(kit->Valid()){
    std::string key,val;
    key.assign(kit->key().data(),kit->key().size());
    val.assign(kit->value().data(),kit->value().size());
    std::cout << "key:" << key << std::endl
              << "val:" << val << std::endl;
    kit->Next();
  }
  delete kit;

  s = n->RangeDel("A","x",100);
  assert(s.ok()); 

  s = n->HGet("Key", "field1", &res);
  assert(s.IsNotFound());

  //sleep(3600);

  delete n;

  return 0;
}
