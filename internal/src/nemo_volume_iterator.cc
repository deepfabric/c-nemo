#include "nemo_volume_iterator.h"
#include <algorithm>
//#include <iostream>
//volume scan

nemo::VolumeIterator::~VolumeIterator(){          
  delete Hit;
  delete Lit;
  delete Sit;
  delete Zit; 
  delete Kit;
}
nemo::VolumeIterator::VolumeIterator(Nemo * nemo, const std::string  &start,const std::string &end, uint64_t limit, bool use_snapshot):
                    n(nemo),end_(end),ends_(end),limit_(limit),count(0),kvt(0){
  Hit = n->HmetaScan(start,end,limit,use_snapshot);
  Lit = n->LmetaScan(start,end,limit,use_snapshot);
  Sit = n->SmetaScan(start,end,limit,use_snapshot);
  Zit = n->ZmetaScan(start,end,limit,use_snapshot);  
  Kit = n->KScanRO(start,end,limit,use_snapshot);
  kvt.resize(0);
  Init();
}

inline bool cmp(const nemo::KVT & kvt1, const nemo::KVT & kvt2){
  if(kvt1.key.compare(kvt2.key)>0)
    return true;
  else
    return false;
}

void nemo::VolumeIterator::Init(){
  valid_ = false;

  if(Hit->Valid())
  {
    kvt.push_back(KVT{Hit->key(),Hit->volume(),DataType::kHSize});
  }
  if(Lit->Valid())
  {
    kvt.push_back(KVT{Lit->key(),Lit->volume(),DataType::kLMeta});
  }
  if(Sit->Valid())
  {
    kvt.push_back(KVT{Sit->key(),Sit->volume(),DataType::kSSize});
  }
  if(Zit->Valid())
  {
    kvt.push_back(KVT{Zit->key(),Zit->volume(),DataType::kZSize});
  }  
  if(Kit->Valid())
  {
    int64_t volume = Kit->value().size() + Kit->key().size();
    kvt.push_back(KVT{Kit->key(), volume, DataType::kKv});
  }
  make_heap(kvt.begin(),kvt.end(),cmp);
  if(kvt.size()>0)
  {
    if(ends_.compare(kvt[0].key)>0){
      valid_ = true;
      count++;
    }
  }
/*
  for(KVT tmp: kvt){
      std::cout <<"after init " << tmp.key.ToString() <<" " << tmp.volume <<" "<< tmp.kType << std::endl;
  }
*/
}

void nemo::VolumeIterator::Next(){
  char nextType = 0;
  if(kvt.size()>0){
    nextType = kvt.begin()->kType;
    pop_heap(kvt.begin(),kvt.end(),cmp);
    kvt.pop_back();
  }
  switch(nextType){
    case  DataType::kHSize:
      Hit->Next();
      if(Hit->Valid() && (ends_.compare(Hit->key())>0)){
          kvt.push_back(KVT{Hit->key(),Hit->volume(),DataType::kHSize});
          push_heap(kvt.begin(),kvt.end(),cmp);
      }
      break;
    case  DataType::kLMeta:
      Lit->Next();
      if(Lit->Valid() && (ends_.compare(Lit->key()))>0){
          kvt.push_back(KVT{Lit->key(),Lit->volume(),DataType::kLMeta});
          push_heap(kvt.begin(),kvt.end(),cmp);
      }
      break;
    case  DataType::kSSize:
      Sit->Next();
      if(Sit->Valid() && (ends_.compare(Sit->key()))>0){
          kvt.push_back(KVT{Sit->key(),Sit->volume(),DataType::kSSize});
          push_heap(kvt.begin(),kvt.end(),cmp);
      }
      break;     
    case  DataType::kZSize:
      Zit->Next();
      if(Zit->Valid() && (ends_.compare(Zit->key()))>0){
          kvt.push_back(KVT{Zit->key(),Zit->volume(),DataType::kZSize});
          push_heap(kvt.begin(),kvt.end(),cmp);
      }
      break;        
    case  DataType::kKv:
      Kit->Next();
        if(Kit->Valid()&& (ends_.compare(Kit->key()))>0){
            int64_t volume = 0;
            volume += Kit->value().size()+Kit->key().size();
            kvt.push_back(KVT{Kit->key(),volume,DataType::kKv});
            push_heap(kvt.begin(),kvt.end(),cmp);
      }
      break; 
  }
  if(kvt.size()>0)
  {
    count++;
    if(count<=limit_){
      valid_ = true;
    }
  }
  else{
    valid_ = false;    
  }
}

bool nemo::VolumeIterator::Valid(){
    return valid_;
}

rocksdb::Slice nemo::VolumeIterator::key(){
  if(kvt.size()>0)
    return kvt.begin()->key;
  else
    return "";
}

int64_t nemo::VolumeIterator::value(){
  if(kvt.size()>0)
    return kvt.begin()->volume;
  else
    return 0;
}

char nemo::VolumeIterator::type(){
  if(kvt.size()>0)
    return kvt.begin()->kType;
  else
    return 0;
}

bool nemo::VolumeIterator::targetScan(int64_t target){
  int64_t totalVol = 0;
  while(valid_){
    totalVol += value();
    if(totalVol>=target){
      targetKey_.assign(key().data(),key().size());
      return true;
    }
    Next();
  }
  totalVol_ = totalVol;
  return false;
}

int64_t nemo::VolumeIterator::totalVolume(){
  return totalVol_;
}

std::string nemo::VolumeIterator::targetKey(){
  return targetKey_;
}

nemo::Status nemo::Nemo::RangeDel(const std::string  & start, const std::string & end, uint64_t limit){
    nemo::Status s;

    KIteratorRO* kit = KScanRO(start,end,limit,true);
    while(kit->Valid()){
      s = kv_db_->Delete(rocksdb::WriteOptions(), kit->key());
      if(s.ok()){
        kit->Next();
      }
      else{
        delete kit;
        return s;
      }
    }
    delete kit;

    HmetaIterator * Hit = HmetaScan(start,end,limit,true);
    HashMeta hmeta;
    std::string meta_val;
    hmeta.len = 0;
    hmeta.vol = 0;
    hmeta.EncodeTo(meta_val);
    while(Hit->Valid()){
      s = hash_db_->PutWithKeyVersion(rocksdb::WriteOptions(), Hit->rawkey(),meta_val);
      if(s.ok()){
        Hit->Next();
      }
      else{
        delete Hit;        
        return s;
      }
    }
    delete Hit;

    LmetaIterator *Lit = LmetaScan(start,end,limit,true);
    ListMeta lmeta = ListMeta();
    lmeta.EncodeTo(meta_val);
    while(Lit->Valid()){
      s = list_db_->PutWithKeyVersion(rocksdb::WriteOptions(), Lit->rawkey(),meta_val);
      if(s.ok()){
        Lit->Next();
      }
      else{
        delete Lit;        
        return s;
      }
    }
    delete Lit;

    SmetaIterator * Sit = SmetaScan(start,end,limit,true);
    SetMeta smeta;
    smeta.len = 0;
    smeta.vol = 0;
    smeta.EncodeTo(meta_val);
    while(Sit->Valid()){
      s = set_db_->PutWithKeyVersion(rocksdb::WriteOptions(), Sit->rawkey(),meta_val);
      if(s.ok()){
        Sit->Next();
      }
      else{
        delete Sit;
        return s;
      }
    }
    delete Sit;

    ZmetaIterator * Zit = ZmetaScan(start,end,limit,true);
    ZSetMeta zmeta;
    zmeta.len = 0;
    zmeta.vol = 0;
    zmeta.EncodeTo(meta_val);
    while(Zit->Valid()){
      s = zset_db_->PutWithKeyVersion(rocksdb::WriteOptions(), Zit->rawkey(),meta_val);
      if(s.ok()){
        Zit->Next();
      }
      else{
        delete Zit;
        return s;
      }
    }
    delete Zit;

    return nemo::Status::OK();
}

nemo::Status nemo::Nemo::RangeDelWithHandle(rocksdb::DBNemo * db,const std::string  & start, const std::string & end, uint64_t limit){
    KIteratorRO * it = KScanWithHandle(db,start,end,limit);
    nemo::Status s;
    for(;it->Valid();it->Next()){
            s = db->Delete(rocksdb::WriteOptions(),it->key());
            if(!s.ok()){
                delete it;
                return s;
            }
    }
    delete it;
    return nemo::Status::OK();
}
