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

    s = n->GetWithHandle(d1,"put",&res);
    assert(s.ok());
*/

/*
    std::cout<< "GetWithHandle after new nemo, res:" << res << std::endl;
    std::cout<< "Kscan with handle:"<< std::endl;
    KIterator * it = n->KScanWithHandle(d1,"A","x",100,true);
    for(int i=0;it->Valid();it->Next(),i++){
        std::cout<<"iterator loops "<< i <<"key:"<< it->key()
                <<",value:" << it->value()
                <<std::endl;
    }
    delete it;
*/
/*
    std::cout<< "Volume Scan:"<< std::endl;
    VolumeIterator * vit = new VolumeIterator(n,"A","x",100,true);
    for(int i=0;vit->Valid();vit->Next(),i++){
        std::cout<<"iterator loops "<< i <<"key:"<< vit->key()
                <<",value:" << vit->value()
                <<std::endl;
    }
    delete vit;
*/

/*
    std::cout<< "redis kV Scan:"<< std::endl;
    KIterator* kit = n->KScan("A","x",100,true);
    for(int i=0;kit->Valid();kit->Next(),i++){
        std::cout<<"iterator loops "<< i <<"key:"<< kit->key()
                <<",value:" << kit->value()
                <<std::endl;
    }
    delete kit;
*/
    log_info("======KV Set======");
    s = n->Set("H0", "h0");
    assert(s.ok());
    s = n->Set("H2", "hk",123);
    assert(s.ok());    
    s = n->Set("H4", "hk",4123);
    assert(s.ok());

    log_info("======Test HSet======");
    s = n->HSet("H1", "h1", "h1");
    assert(s.ok());

    s = n->HSet("H2", "h2", "h2");
    assert(s.ok());     

    s = n->HSet("H2", "hello", "world");
    assert(s.ok()); 

    s = n->HSet("H2", "h1", "h1");
    assert(s.ok());
    s = n->HDel("H2", "h1");
    assert(s.ok());

    int64_t count;
    s = n->Del("H1",&count);
    s = n->HSet("H1", "h1", "h1");
    assert(s.ok());

    s = n->HSet("H3", "h3", "h3");
    assert(s.ok()); 
    int64_t ts = 0;
    s = n->Expireat("H3",2147483000,&ts);
    assert(s.ok()); 
    log_info("Expireat at timestamp:%d",ts);
    log_info("");

    s = n->TTL("H3",&ts);    
    assert(s.ok()); 
    log_info("H3 TTL:%d",ts);
    log_info("");   
    n->RawScanSave(kHASH_DB,"A","x",true);

 /*   

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
/*
    s = RangeDelWithHandle(n,d1,"A","x",100);
    assert(s.ok()); 

    s = n->GetWithHandle(d1,"put",&res);
    assert(s.IsNotFound());
*/

    delete n;
    return 0;
}

