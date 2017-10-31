#include <iostream>
#include <assert.h>
#include "xdebug.h"
#include <string>
#include "nemo.h"

using namespace nemo;

void Usage() {
  std::cout << "Usage: " << std::endl;
  std::cout << "./hash_meta_check db_path start_key end_key" << std::endl;
  std::cout << "Example: " << std::endl;
  std::cout << "./meta_scan ./db k1 kn" << std::endl;
}

int main(int argc, char **argv)
{
  if (argc != 4) {
    Usage();
    log_err("not enough parameter");
  }
  std::string path(argv[1]);
  std::string start_key(argv[2]);
  std::string end_key(argv[3]);
  // Create nemo handle
  nemo::Options option;
  option.write_buffer_size = 268435456;
  option.target_file_size_base = 20971520;
  log_info("Prepare DB...");
  nemo::Nemo* n = new nemo::Nemo(path, option);
  assert(n);
  log_info("Check Begin");

  HmetaIterator* Hit = n->HmetaScan(start_key,end_key,1LL << 60,false);
  int count = 0;
  int sum = 0;
  while(Hit->Valid()){
    Status s = n->HCheckMetaKey(std::string(Hit->key().data(),Hit->key().size()));
    sum++;
    if(!s.ok()){
      std::cout<< s.ToString();
      count++;
    }
    Hit->Next();
  }
  delete n;

  log_info("Check Finshed");
  if(count == 0){
    std::cout<< sum <<" keys are checked. No key is wrong" << std::endl;
  }
  else{
    std::cout<< sum <<" keys are checked. There is total wrong meta keys: " << count << std::endl;
  }
  return 0;
}

