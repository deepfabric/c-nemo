#include <iostream>
#include <vector>
#include <string>
#include <ctime>
#include <inttypes.h>
#include <limits>

#include "nemo.h"
#include "xdebug.h"

using namespace nemo;

Nemo *n;

int main() {
  nemo::Options options;
  options.target_file_size_base = 20 * 1024 * 1024;

  n = new Nemo("/tmp/nemo_simple_test", options); 

  std::string res;
  Status s ;

  s = n->HSet("Key", "field1", "val1");
  s = n->HSet("Key", "field2", "val2");
  std::cout << "HSet return: " << s.ToString() << std::endl;

  int64_t l = n->HLen("Key");
  std::cout << "HLen return: " << l << std::endl;

  rocksdb::DBNemo * meta = n->GetMetaHandle();

  s = n->PutWithHandle(meta,"Hello","World");
  assert(s.ok());
  s = n->GetWithHandle(meta,"Hello",&res);
  assert(s.ok());
  assert(res == "World");
  rocksdb::WriteBatch wb;
  wb.Put("MetaKey1","MetaVal1");
  wb.Put("MetaKey2","MetaVal2");
  s = n->BatchWrite(meta,&wb);
  assert(s.ok());
  s = n->GetWithHandle(meta,"MetaKey2",&res);
  assert(s.ok());  
  assert(res == "MetaVal2"); 

  KIterator* kit = n->KScanWithHandle(meta,"A","x",100,true);
  while(kit->Valid()){
    std::string key,val;
    key.assign(kit->key().data(),kit->key().size());
    val.assign(kit->value().data(),kit->value().size());
    std::cout << "key:" << key << std::endl
              << "val:" << val << std::endl;
    kit->Next();
  }
  delete kit;
  delete n;

  return 0;
}
