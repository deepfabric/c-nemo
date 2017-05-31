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
std::string db_path = "/tmp/nemo/sst_test";
std::string db_dump_path = "/tmp/nemo/sst_test_dump";
int main()
{
    nemo::Options options;
    options.target_file_size_base = 20 * 1024 * 1024;
    system("rm -rf /tmp/nemo/sst_test");
    system("rm -rf /tmp/nemo/sst_test_dump");
    //options.compression = false; 

    Nemo *n = new Nemo(db_path, options); 
    Status s;
    std::string res;
    int64_t count;    

    log_info("======KV Set======");
    s = n->Set("K1", "V1");
    assert(s.ok());
    s = n->Set("K2", "V2",4321);
    assert(s.ok());    

    log_info("======Test HSet======");
    int HSetRes;
    s = n->HSet("H1", "h1", "h1", &HSetRes);
    assert(s.ok());
    s = n->HSet("H2", "h2", "h2", &HSetRes);
    assert(s.ok());     
    s = n->HSet("H2", "hello", "world", &HSetRes);
    assert(s.ok()); 
    s = n->HSet("H2", "h1", "h1", &HSetRes);
    assert(s.ok());
    s = n->HDel("H2", "h1");
    assert(s.ok());
    s = n->Del("H1",&count);
    assert(s.ok());    
    s = n->HSet("H1", "h1", "h1", &HSetRes);
    assert(s.ok());
    s = n->HSet("H3", "h3", "h3", &HSetRes);
    assert(s.ok()); 
    int64_t ts = 0;
    s = n->Expireat("H3",2147483000,&ts);
    assert(s.ok()); 
    s = n->TTL("H3",&ts);
    assert(s.ok());
    log_info("Expireat at timestamp:%d",ts);
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

    int64_t res_set;
    s = n->SAdd("S1","s1A",&res_set);
    assert(s.ok());
    s = n->SAdd("S1","s1B",&res_set);
    assert(s.ok());     
    s = n->SAdd("S2","s2",&res_set);
    assert(s.ok());     

    int64_t res_zset;
    double score;
    s = n->ZAdd("Z1",100.00,"z1A",&res_zset);
    assert(s.ok());
    s = n->ZAdd("Z1",200.00,"z1B",&res_zset);
    assert(s.ok());     
    s = n->ZAdd("Z2",300.00,"z2",&res_zset);
    assert(s.ok()); 

    s = n->RawScanSaveAll(db_dump_path,"A","zz",true);
    assert(s.ok());

    std::cout<< "Volume Scan before ingest:"<< std::endl;
    VolumeIterator * vit = new VolumeIterator(n,"A","zz",100,true);
    for(int i=0;vit->Valid();vit->Next(),i++){
        std::cout<<"iterator loops "<< i <<"key:"<< vit->key()
                <<",value:" << vit->value()
                <<",type:" << vit->type()
                <<std::endl;
    }
    delete vit;

    delete n;

    system("rm -rf /tmp/nemo/sst_test");
    n = new Nemo(db_path, options);

    s = n->Set("K1","V1 before Ingest");
    assert(s.ok());

    s = n->IngestFile(db_dump_path);
    assert(s.ok());

    std::cout<< "Volume Scan after ingest:"<< std::endl;
    vit = new VolumeIterator(n,"A","zz",100,true);
    for(int i=0;vit->Valid();vit->Next(),i++){
        std::cout<<"iterator loops "<< i <<"key:"<< vit->key()
                <<",value:" << vit->value()
                <<",type:" << vit->type()
                <<std::endl;
    }
    delete vit;

    std::string res_kv;
    s = n->Get("K1",&res_kv);
    assert(s.ok());
    log_info("After ingest,K1:%s",res_kv.c_str());
    assert(res_kv == "V1");
    s = n->Get("K2",&res_kv);
    assert(s.ok());
    assert(res_kv == "V2");
    int64_t kv_ttl; 
    s = n->TTL("K2",&kv_ttl);
    assert(s.ok()); 
//    sleep(1);
//    assert(kv_ttl == 4320);

//    log_info("HashRawScan:");
//    n->HashRawScan("A","x",true);

    s = n->HGet("H1", "h1", &res);
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

    bool isMember;
    n->SIsMember("S1","s1A",&isMember);
    assert(isMember);
    n->SIsMember("S1","s1B",&isMember);
    assert(isMember);
    n->SIsMember("S2","s2",&isMember);
    assert(isMember);          

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
    return 0;
}

