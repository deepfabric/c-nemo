#include <iostream>
#include <vector>
#include <string>
#include <ctime>
#include <inttypes.h>
#include <limits>

#include "nemo.h"
#include "xdebug.h"

#include "rocksdb/db.h"

using namespace nemo;
std::string db_path = "/tmp/nemo/sst_test";
std::string db_dump_path = "/tmp/nemo/sst_test_dump";
int main()
{
    nemo::Options options;
    options.target_file_size_base = 20 * 1024 * 1024;
    system("rm -rf /tmp/nemo/sst_test");
    system("rm -rf /tmp/nemo/sst_test_dump");
    //options.compression = false; 

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
    Nemo *n = new Nemo(db_path, options); 
    Status s;
    std::string res;
    int64_t count;    

    log_info("======KV Set======");
    s = n->Set("K1", "V1");
    assert(s.ok());
    s = n->Set("K2", "V2",12345);
    assert(s.ok());
    s = n->KvRawScanSave(db_dump_path,"A","x",true);
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
    s = n->Del("H1",&count);
    s = n->HSet("H1", "h1", "h1");
    assert(s.ok());
    s = n->HSet("H3", "h3", "h3");
    assert(s.ok()); 
    int64_t ts = 0;
    s = n->Expireat("H3",2147483000,&ts);
    assert(s.ok()); 
    s = n->TTL("H3",&ts);
    assert(s.ok());
    log_info("Expireat at timestamp:%d",ts);
    s = n->HashRawScanSave(db_dump_path,"A","x",true);
    assert(s.ok()); 

    int64_t list_len;
    n->LPush("L1","l1A",&list_len);
    assert(s.ok());     
    n->LPush("L1","l1B",&list_len);
    assert(s.ok()); 
    n->LPush("L2","l2A",&list_len);
    assert(s.ok());
    n->LPush("L3","l3A",&list_len);
    assert(s.ok()); 
    s = n->Del("L3",&count);
    assert(s.ok()); 
    s = n->ListRawScanSave(db_dump_path,"A","x",true);
    assert(s.ok()); 

    int64_t res_set;
    s = n->SAdd("S1","s1A",&res_set);
    assert(s.ok());
    s = n->SAdd("S1","s1B",&res_set);
    assert(s.ok());     
    s = n->SAdd("S2","s2",&res_set);
    assert(s.ok());     
    s = n->SetRawScanSave(db_dump_path,"A","x",true);
    assert(s.ok()); 

    int64_t res_zset;
    double score;
    s = n->ZAdd("Z1",100.00,"z1A",&res_zset);
    assert(s.ok());
    s = n->ZAdd("Z1",200.00,"z1B",&res_zset);
    assert(s.ok());     
    s = n->ZAdd("Z2",300.00,"z2",&res_zset);
    assert(s.ok());     
    s = n->ZsetRawScanSave(db_dump_path,"A","x",true);
    assert(s.ok());

    delete n;

    system("rm -rf /tmp/nemo/sst_test");
    n = new Nemo(db_path, options);
    s = n->IngestFile(db_dump_path);
    assert(s.ok());

    std::string res_kv;
    s = n->Get("K1",&res_kv);
    assert(s.ok());
    assert(res_kv == "V1");    
    s = n->Get("K2",&res_kv);
    assert(s.ok());
    assert(res_kv == "V2");
    int64_t kv_ttl; 
    s = n->TTL("K2",&kv_ttl);
    assert(s.ok()); 
    assert(kv_ttl == 12345);     

//    log_info("HashRawScan:");
//    n->HashRawScan("A","x",true);

    s = n->HGet("H1", "h1", &res);
    log_info("HGet error:%s",s.ToString().c_str());      
    assert(s.ok());
    assert(res=="h1");
    s = n->HGet("H2", "h2", &res);
    assert(s.ok());
    assert(res=="h2");
    s = n->HGet("H2", "h1", &res);
    assert(s.IsNotFound());
    s = n->HGet("H3", "h3", &res);
    assert(s.ok());
    assert(res=="h3");
    int64_t new_ts;
    s = n->TTL("H3",&new_ts);
    assert(s.ok());

    std::string list_val;
    s = n->LPop("L1",&list_val);
    assert(s.ok());   
    assert("l1B"==list_val);
    s = n->LPop("L1",&list_val);
    assert(s.ok());     
    assert("l1A"==list_val);

    s = n->LPop("L2",&list_val);
    assert(s.ok());     
    assert("l2A"==list_val);

    s = n->LPop("L3",&list_val);
    assert(s.IsNotFound()); 

    s = n->ZScore("Z1","z1A",&score);
    assert(s.ok());
    assert(score == 100.00);  
    s = n->ZScore("Z1","z1B",&score);
    assert(s.ok());
    assert(score == 200.00);  
    s = n->ZScore("Z2","z2",&score);
    assert(s.ok());
    assert(score == 300.00);  

    log_info("Ingest OK!");
    log_info(""); 
    delete n; 
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
    return 0;
}

