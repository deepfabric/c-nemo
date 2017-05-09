#include <iostream>
#include <vector>
#include <string>
#include <ctime>
#include <inttypes.h>
#include <limits>

#include "nemo.h"
#include "xdebug.h"

#include "rocksdb/db.h"

#include "nemo_volume_iterator.h"

using namespace nemo;

int main()
{
    nemo::Options options;
    options.target_file_size_base = 20 * 1024 * 1024;
    system("rm -rf /tmp/nemo/volume_iterator");
    //options.compression = false; 
    Nemo *n = new Nemo("/tmp/nemo/volume_iterator", options); 
    Status s;

    std::string res;
    /*
     *************************************************KV**************************************************
     */
    /*
     *  Test Internal Hash Skip
     */
/*     
    log_info("======Test Set======");
    s = n->Set("H0", "h0");
    assert(s.ok());
    s = n->Set("H2", "hk");
    assert(s.ok());    
    s = n->Set("H4", "hk");
    assert(s.ok());
*/
    rocksdb::DBWithTTL * d1 = n->GetMetaHandle();
    rocksdb::DBWithTTL * d2 = n->GetRaftHandle();

    rocksdb::WriteBatch batch;
    batch.Put("key1", "value1");
    batch.Put("key2", "value2");
    batch.Put("key3", "value3");
    batch.Delete("key1");

    s = n->BatchWrite(d1,&batch);
    assert(s.ok());     
    s = n->BatchWrite(d2,&batch);
    assert(s.ok()); 

    s = n->PutWithHandle(d1,"put","put");
    assert(s.ok());
    s = n->GetWithHandle(d1,"put",&res);
    assert(s.ok());
    assert( res == "put");

    s = n->GetWithHandle(d1,"key2",&res);
    assert(s.ok());
    assert( res == "value2");    

    KIterator * it = n->KScanWithHandle(d1,"A","x",100,false);
    for(int i=0;it->Valid();it->Next(),i++){
        std::cout<<"iterator loops "<< i <<"key:"<< it->key()
                <<",value:" << it->value()
                <<std::endl;
    } 
    delete it;

//    s = n->Set("H2", "hk");
//    assert(s.ok());
/*
    log_info("======Test HSet======");
    s = n->HSet("H1", "h1", "h1");
    assert(s.ok());
    s = n->HSet("H2", "h1", "h1");
    assert(s.ok());     

    s = n->HSet("H2", "h2", "h2");
    assert(s.ok());
    s = n->HSet("H2", "h2", "h2");
    assert(s.ok());

    s = n->HDel("H2", "h2");
    assert(s.ok());

    s = n->HSet("H3", "h1", "h1");
    assert(s.ok()); 

    s = n->Get("H2",&res);
    std::cout<<"Get H2 res: " <<s.ToString()<<std::endl;
    assert(s.ok());

*/
/*
//Test Expire

    int64_t ret; 
    log_info("======Test Del======");
    s = n->Expire("H1", 1, &ret);
    log_info("Test Expire OK return %s", s.ToString().c_str());
    sleep(1);


// Test Del

    log_info("======Test Del======");
    s = n->Del("H3", &ret);
    log_info("Test Del OK return %s", s.ToString().c_str());
*/
/*
    VolumeIterator * it = new VolumeIterator(n,"","X",100);
    for(int i=0;it->Valid();it->Next(),i++){
        std::cout<<"iterator loops "<< i <<" entry name:"<< it->key()
                <<",entry vol:" << it->value()
                <<",entry type:" << it->type()
                <<std::endl;
    } 
*/
    s = RangeDelWithHandle(n,d1,"A","x",100);
    assert(s.ok()); 

    s = n->GetWithHandle(d1,"put",&res);
    assert(s.IsNotFound());
/*
    it = n->KScanWithHandle(d1,"","X",100,true);
    for(int i=0;it->Valid();it->Next(),i++){
        std::cout<<"iterator loops "<< i <<"key:"<< it->key()
                <<",value:" << it->value()
                <<std::endl;
    } 
*/
    delete n;
    return 0;
}

