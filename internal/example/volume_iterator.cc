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

    log_info("======KV Set======");
    s = n->Set("H0", "h0");
    assert(s.ok());
    s = n->Set("H2", "hk",123);
    assert(s.ok());    
    s = n->Set("H4", "hk",4123);
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
    int64_t count;
    s = n->Del("H1",&count);
    s = n->HSet("H1", "h1", "h1", &HSetRes);
    assert(s.ok());
    s = n->HSet("H3", "h3", "h3", &HSetRes);
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

    log_info("======Test LPush======");
    int64_t llen = 0;
    s = n->LPush("tLPushKey", "tLPushVal1", &llen);
    assert(s.ok()); 
    assert(llen==1);
    s = n->LPush("tLPushKey", "tLPushVal2", &llen);
    assert(s.ok());
    assert(llen==2);  
    log_info("");
    /*
     *  Test LLen
     */
    log_info("======Test LLen======");
    s = n->LLen("tLPushKey", &llen);
//    std::cout<< "LLen res:"<< s.ToString() <<std::endl;    
    assert(s.ok());    
    assert(llen==2);
    log_info("");
    /*
     *  Test LPop
     */
    log_info("======Test LPop======");
    res = "";
    s = n->LPop("tLPushKey", &res);
    assert(s.ok());
    assert(res == "tLPushVal2");
    log_info("");

    /*
     *  Test SAdd
     */
    int64_t sadd_res;  
    log_info("======Test Set======");
    s = n->SAdd("setKey", "member1", &sadd_res);
    assert(s.ok());
    s = n->SAdd("setKey", "member2", &sadd_res);
    assert(s.ok()); 
    s = n->SRem("setKey", "member2", &sadd_res);
    assert(s.ok());

    /*
     *  Test ZAdd
     */
    int64_t zadd_res; 
    log_info("======Test ZSet======");
    s = n->ZAdd("zsetKey", 1.234, "member1", &zadd_res);
    assert(s.ok());
    s = n->ZAdd("zsetKey", 2.345, "member2", &zadd_res);
    assert(s.ok()); 
    s = n->ZRem("zsetKey", "member2", &zadd_res);
    assert(s.ok());

    std::cout<< "Volume Scan:"<< std::endl;
    VolumeIterator * vit = new VolumeIterator(n,"A","zz",100,true);
    for(int i=0;vit->Valid();vit->Next(),i++){
        std::cout<<"iterator loops "<< i <<"key:"<< vit->key()
                <<",value:" << vit->value()
                <<",type:" << vit->type()
                <<std::endl;
    }
    delete vit;

    delete n;
    return 0;
}

