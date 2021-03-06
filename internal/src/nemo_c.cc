#include "nemo_c.h"
#include "nemo.h"

#include "nemo_options.h"
#include "nemo_const.h"
#include "nemo_iterator.h"
#include "nemo_volume_iterator.h"
//#include "nemo_meta.h"
//#include "nemo_backupable.h"

#include "port.h"
#include "util.h"
#include "xdebug.h"
#include "rocksdb/write_batch.h"

#include <iostream>

using nemo::Nemo;
using nemo::Options;

using nemo::DBType;
using nemo::Position;
using nemo::Aggregate;
using nemo::Iterator;
using nemo::KIterator;
using nemo::KIteratorRO;
using nemo::HIterator;
using nemo::HmetaIterator;
using nemo::ZIterator;
using nemo::SIterator;
using nemo::KV;
using nemo::KVS;
using nemo::KVSlice;
using nemo::SS;
using nemo::FV;
using nemo::FVS;
using nemo::FVSlice;
using nemo::IV;
using nemo::SM;
using nemo::BitOpType;
using nemo::Snapshots;
using nemo::Snapshot;
//using nemo:MetaPtr;

using rocksdb::Status;

extern "C"	{

	struct nemo_t {	Nemo * rep;	};
	struct nemo_options_t { Options rep; };
	struct nemo_Iterator_t { Iterator * rep;};	
	struct nemo_KIterator_t { KIterator * rep;};
	struct nemo_KIteratorRO_t { KIteratorRO * rep;};
	struct nemo_HIterator_t { HIterator * rep;};
	struct nemo_HmetaIterator_t { HmetaIterator * rep;};
	struct nemo_ZIterator_t { ZIterator * rep;};
	struct nemo_SIterator_t { SIterator * rep;};
	struct nemo_Snaptshot_t { Snapshot * rep;};
	struct nemo_VolumeIterator_t { nemo::VolumeIterator * rep;};
	struct nemo_DBNemo_t {rocksdb::DBNemo * rep;};
	struct nemo_WriteBatch_t { rocksdb::WriteBatch rep;};

	void nemo_delCppStr(void * p)
	{
		delete ((std::string *) p);
	}

	void nemo_delSSVector(void * p)
	{
		std::vector<SS> * ssp = (std::vector<SS> *) p;
		for(size_t i=0;i<(*ssp).size();i++){
			delete (*ssp)[i].val;
		}
		delete ssp;
	}

//	struct nemo_MetaPtr { MetaPtr rep};
#ifdef __GO_WRAPPER__
	static char* CopyString(const std::string& str) {
	  char* result = reinterpret_cast<char*>(malloc(sizeof(char) * str.size()));
	  memcpy(result, str.data(), sizeof(char) * str.size());
	  // There is no '\0' in origin CopyString in file: rocksdb/db/c.cc
	  // used by go,return string wiout '\0'
	  return result;
	}
#else
        static char* CopyString(const std::string& str) {
          char* result = reinterpret_cast<char*>(malloc(sizeof(char) *( str.size()+1)));
          memcpy(result, str.data(), sizeof(char) * str.size());
          result[str.size()]='\0';
	  // used by pure c api, string must be ended with '\0'
          // There is no '\0' in origin CopyString in file: rocksdb/db/c.cc
          return result;
        }
#endif

	static bool nemo_SaveError(char** errptr, const Status& s) {
	  assert(errptr != nullptr);
	  if (s.ok()) {
	    return false;
	  } else if (*errptr == nullptr) {
	    *errptr = strdup(s.ToString().c_str());
	  } else {
	    // TODO(sanjay): Merge with existing error?
	    // This is a bug if *errptr is not created by malloc()
	    free(*errptr);
	    *errptr = strdup(s.ToString().c_str());
	  }
	  return true;
	}


	nemo_t * nemo_Create(const char * db_path,const nemo_options_t * options){
		Nemo * nemo_instance_p = new Nemo(std::string(db_path),options->rep);
		nemo_t * nemo = new nemo_t;
		nemo->rep = nemo_instance_p;
		return nemo;
	}

	nemo_options_t * nemo_CreateOption()
	{
		return new nemo_options_t;

	}

	void nemo_SetOptions(nemo_options_t * cOpts,GoNemoOpts * goOpts)
	{
		cOpts->rep.create_if_missing = goOpts->create_if_missing;
		cOpts->rep.write_buffer_size = goOpts->write_buffer_size;
		cOpts->rep.max_open_files 	 = goOpts->max_open_files;
		cOpts->rep.use_bloomfilter   = goOpts->use_bloomfilter;
		cOpts->rep.write_threads     = goOpts->write_threads;
		cOpts->rep.target_file_size_base 	      = goOpts->target_file_size_base;
		cOpts->rep.target_file_size_multiplier    = goOpts->target_file_size_multiplier;
		cOpts->rep.compression 				      = goOpts->compression;
		cOpts->rep.max_background_flushes         = goOpts->max_background_flushes;
		cOpts->rep.max_background_compactions     = goOpts->max_background_compactions;
		cOpts->rep.max_bytes_for_level_multiplier = goOpts->max_bytes_for_level_multiplier;

		cOpts->rep.max_bytes_for_level_base 			= goOpts->max_bytes_for_level_base;
		cOpts->rep.level0_slowdown_writes_trigger 		= goOpts->level0_slowdown_writes_trigger;
		cOpts->rep.level0_stop_writes_trigger 			= goOpts->level0_stop_writes_trigger;
		cOpts->rep.min_write_buffer_number_to_merge 	= goOpts->min_write_buffer_number_to_merge;
		cOpts->rep.level0_file_num_compaction_trigger 	= goOpts->level0_file_num_compaction_trigger;
		cOpts->rep.delayed_write_rate 					= goOpts->delayed_write_rate;
		cOpts->rep.max_write_buffer_number 				= goOpts->max_write_buffer_number;

		cOpts->rep.disable_wal                          = goOpts->disable_wal;

	}

	void nemo_Compact(nemo_t * nemo,int db_type,bool sync,char ** errptr){
		nemo_SaveError(errptr,nemo->rep->Compact(static_cast<nemo::DBType>(db_type),sync));
	}

	void nemo_RunBGTask(nemo_t * nemo,char ** errptr){
		nemo_SaveError(errptr,nemo->rep->RunBGTask());
	}

	char * nemo_GetCurrentTaskType(nemo_t * nemo){
		return strdup(nemo->rep->GetCurrentTaskType().c_str());
	}

	void nemo_free(nemo_t * instance){
		delete  instance->rep;
	}

	// =================String=====================
	void nemo_Del(nemo_t * nemo,const char * key, const size_t keylen ,int64_t  *count,char ** errptr){
	 	nemo_SaveError(errptr,nemo->rep->Del(std::string(key,keylen),count));
	}

	void nemo_MDel(nemo_t * nemo,const int num, const char ** key_list,const size_t * key_list_size, int64_t *count,char ** errptr){
		int64_t res;
	 	std::vector<std::string> keys(num);
	 	for(int i = 0;i<num;i++)
	 		keys[i] = std::string(key_list[i],key_list_size[i]);

	 	nemo_SaveError(errptr,nemo->rep->MDel(keys,&res));
		*count = res;
	}

	void nemo_Expire(nemo_t * nemo,const char * key, const size_t keylen, int32_t  seconds, int64_t * res,char ** errptr){
		nemo_SaveError(errptr,nemo->rep->Expire(std::string(key, keylen),seconds,res));
	}

	void nemo_TTL(nemo_t * nemo,const char * key,const size_t keylen, int64_t *res,char ** errptr){
		nemo_SaveError(errptr,nemo->rep->TTL(std::string(key,keylen),res));
	}

	void nemo_Persist(nemo_t * nemo,const char * key, const size_t keylen, int64_t *res,char ** errptr){
		nemo_SaveError(errptr,nemo->rep->Persist(std::string(key, keylen),res));
	}

	void nemo_Expireat(nemo_t * nemo,const char * key,const size_t keylen, const int32_t  timestamp, int64_t  *res,char ** errptr){
		nemo_SaveError(errptr,nemo->rep->Expireat(std::string(key,keylen),timestamp,res));
	}

	void nemo_Type(nemo_t * nemo,const char * key, const size_t keylen, char ** type,char ** errptr){
		std::string res;
		nemo_SaveError(errptr,nemo->rep->Type(std::string(key, keylen),&res));
		*type = strdup(res.c_str());
	}

	void nemo_Exists(nemo_t * nemo,const int num, const char ** key_list,const size_t * key_list_size,int64_t * res,char ** errptr){

		std::vector<std::string> keys(num);
		for(int i=0;i<num;i++)
			keys[i] = std::string(key_list[i],key_list_size[i]);

		nemo_SaveError(errptr,nemo->rep->Exists(keys,res));

	}

	// =================KV=====================
	void nemo_Set(nemo_t * nemo,const char * key, const size_t keylen, const char * val, const size_t vallen, int32_t ttl, char ** errptr){
		nemo_SaveError(errptr,nemo->rep->Set(rocksdb::Slice(key,keylen),rocksdb::Slice(val,vallen),ttl));
	}

	void * nemo_Get(nemo_t * nemo,const char * key, const size_t keylen, const char ** val,size_t * vallen, char ** errptr){
		std::string  * res_value = new std::string();
		Status s = nemo->rep->Get(rocksdb::Slice(key,keylen),res_value);

		if(s.ok())
		{
			*errptr = nullptr;
			*val = res_value->data();
			*vallen  = res_value->size();
		}
		else if (s.IsNotFound())
		{
			*errptr = nullptr;
			*val = nullptr ;
			*vallen  = 0;
		}
		else
		{
			*errptr = strdup(s.ToString().c_str());
		}
		return res_value;
	}

	void nemo_MSet(nemo_t * nemo, const int  num,const char ** key,size_t * keylen,const char ** value,size_t * valuelen, char ** errptr){
		std::vector<KVSlice> kv(num);
		for(int i=0;i<num;i++){
			kv[i].key = rocksdb::Slice(key[i],keylen[i]);
			kv[i].val = rocksdb::Slice(value[i],valuelen[i]);
		}
		nemo_SaveError(errptr,nemo->rep->MSetSlice(kv));
	}

	void nemo_WriteBatchTtl(nemo_t * nemo, int const num, const char ** key, size_t * keylen, \
														  const char ** val, size_t * vallen, \
														  int32_t * ops, int32_t * ttls, bool sync, char ** errptr){
		std::vector<rocksdb::KVOT> kvots(num);
		for(int i=0;i<num;i++){
			kvots[i].key = rocksdb::Slice(key[i],keylen[i]);
			kvots[i].val = rocksdb::Slice(val[i],vallen[i]);
			kvots[i].ops = ops[i];
			kvots[i].ttl = ttls[i];
		}
		nemo_SaveError(errptr,nemo->rep->WriteBatchTtl(kvots, sync));
	}

	void * nemo_MGet(nemo_t * nemo,  const int num, const char ** key, size_t * keylen, \
	 		             		                    const char ** val, size_t * vallen, char ** errs){
		std::vector<rocksdb::Slice> keys(num);
		for(int i=0;i<num;i++){
			keys[i] = rocksdb::Slice(key[i],keylen[i]);
		}
		
		std::vector<SS> * vsp = new std::vector<SS>(num);
		char * nemo_res;
		nemo_SaveError(&nemo_res,nemo->rep->MGetSlice(keys,*vsp));
		
		for(int i=0;i<num;i++){
			if((*vsp)[i].status.ok()){
				val[i]   = (*vsp)[i].val->data();
				vallen[i] = (*vsp)[i].val->size();
				errs[i] = NULL;
			}
			else if((*vsp)[i].status.IsNotFound())
			{
				val[i]   = nullptr;
				vallen[i] = 0;
				errs[i] = NULL;					
			}
			else {
				val[i] = NULL;
				vallen[i] = 0;
				errs[i] = strdup((*vsp)[i].status.ToString().c_str());
			}
		}
		return (void *)vsp;
	}
	
	void nemo_Keys(nemo_t * nemo,  char * pattern, const size_t patternlen, int * key_num, \
					char *** key_list, size_t ** key_list_strlen , char ** errptr){
			std::vector<std::string> keys;
			nemo_SaveError(errptr,nemo->rep->Keys(std::string(pattern,patternlen),keys));
			*key_num = keys.size();
			if(*key_num > 0){
					*key_list = new char * [*key_num];
					*key_list_strlen = new size_t [*key_num];
					for(int i=0; i<*key_num; i++){
							(*key_list)[i] = CopyString(keys[i]);
							(*key_list_strlen)[i] = keys[i].size();
					}
			}
			else
			{
					*key_list = nullptr;
					*key_list_strlen = nullptr;
			}
	}

	void nemo_KMDel(nemo_t * nemo,const int num,const char ** key_list,size_t * key_list_size,int64_t *count, char ** errptr){
		std::vector<std::string> keys(num);
		for(int i=0;i<num;i++)
			keys[i] = std::string(key_list[i],key_list_size[i]);
		nemo_SaveError(errptr,nemo->rep->KMDel(keys,count));
	}


	void nemo_Incrby(nemo_t * nemo,const char * key, const size_t keylen, const int64_t by, char ** new_val,size_t * new_val_len,char ** errptr){
		std::string * new_val_p = new std::string();
		nemo_SaveError(errptr,nemo->rep->Incrby(std::string(key,keylen),by,*new_val_p));
		log_info("nemo_Incrby:	new_val_p %s",new_val_p->c_str());
/*

		std::string new_val_cpp;
		nemo_SaveError(errptr,nemo->rep->Incrby(std::string(key),by_cpp,new_val_cpp));
		log_info("nemo_Incrby:	new_val_cpp %s",new_val_cpp.c_str());
*/		
		*new_val = CopyString(*new_val_p); 
		*new_val_len = new_val_p->size();
	}
	void nemo_Decrby(nemo_t * nemo,const char * key, const size_t keylen, const int64_t by, char ** new_val,size_t * new_val_len,char ** errptr){
		std::string new_val_cpp;
		nemo_SaveError(errptr,nemo->rep->Decrby(std::string(key, keylen),by,new_val_cpp));
		*new_val = CopyString(new_val_cpp); 
		*new_val_len = new_val_cpp.size();
	}
	void nemo_Incrbyfloat(nemo_t * nemo,const char * key, const size_t keylen, const double by, char ** new_val,size_t * new_val_len,char ** errptr){
		std::string new_val_cpp;
		nemo_SaveError(errptr,nemo->rep->Incrbyfloat(std::string(key,keylen),by,new_val_cpp));
		*new_val = CopyString(new_val_cpp); 
		*new_val_len = new_val_cpp.size();
	}
	void nemo_GetSet(nemo_t * nemo,const char * key, const size_t keylen, const char * value, const size_t vallen, char ** old_val,size_t * old_val_len,char ** errptr){
		std::string old_val_str;
		nemo_SaveError(errptr,nemo->rep->GetSet(std::string(key, keylen),std::string(value,vallen),&old_val_str));
		*old_val = CopyString(old_val_str);
		*old_val_len = old_val_str.size();
	}
	void nemo_Append(nemo_t * nemo,const char * key, const size_t keylen, const char * value, const size_t vallen,  int64_t * new_len,char ** errptr){
		nemo_SaveError(errptr,nemo->rep->Append(std::string(key, keylen),std::string(value,vallen),new_len));
	}
	void nemo_Setnx(nemo_t * nemo,const char * key, const size_t keylen, const char * value, const size_t vallen,  int64_t * ret,const int32_t ttl,char ** errptr){
		nemo_SaveError(errptr,nemo->rep->Setnx(std::string(key, keylen),std::string(value,vallen),ret,ttl));
	}
	void nemo_Setxx(nemo_t * nemo,const char * key, const size_t keylen, const char * value,  const size_t vallen, int64_t * ret,const int32_t ttl,char ** errptr){
		nemo_SaveError(errptr,nemo->rep->Setxx(std::string(key,keylen),std::string(value,vallen),ret,ttl));
	}
	void nemo_MSetnx(nemo_t * nemo,const int num, const char ** key,const size_t * keylen,	\
												  const char ** val,const size_t * vallen, 	\
												  int64_t * ret,char ** errptr){
		std::vector<KV> kv(num);
		for(int i=0;i<num;i++){
			kv[0].key = std::string(key[i],keylen[i]);
			kv[0].val = std::string(val[i],vallen[i]);
		}
		nemo_SaveError(errptr,nemo->rep->MSetnx(kv,ret));
	}
	void nemo_Getrange(nemo_t * nemo,const char * key, const size_t keylen, const int64_t start,const int64_t end, char ** substr,size_t * substr_len, char ** errptr){
		std::string substr_cpp;
		Status s = nemo->rep->Getrange(std::string(key,keylen),start,end,substr_cpp);
		if(s.ok())
		{
			*errptr = nullptr;
			*substr = CopyString(substr_cpp);
			*substr_len = substr_cpp.size();
		}
		else if (s.IsNotFound())
		{
			*errptr = nullptr;
			*substr = nullptr;
			*substr_len = 0;
		}
		else
		{
			*errptr = strdup(s.ToString().c_str());
		}		

	}
	void nemo_Setrange(nemo_t * nemo,const char * key, const size_t keylen, const int64_t offset,const char * value, const size_t vallen, int64_t * len ,char ** errptr){
		nemo_SaveError(errptr,nemo->rep->Setrange(std::string(key,keylen),offset,std::string(value,vallen),len));
	}
	void nemo_Strlen(nemo_t * nemo,const char * key, const size_t keylen, int64_t * len, char ** errptr){
		Status s = nemo->rep->Strlen(std::string(key,keylen),len);
		if(s.ok()|| s.IsNotFound())
		{
			*errptr = nullptr;
		}
		else
		{
			*errptr = strdup(s.ToString().c_str());
		}	
	}
	nemo_KIterator_t  * nemo_KScan(nemo_t *nemo,const char * start,const size_t startlen, const char * end, const size_t endlen, uint64_t limit,bool use_snapshot){
		uint64_t limit_cpp = limit;
		nemo_KIterator_t * it = new nemo_KIterator_t;
		it->rep = nemo->rep->KScan(std::string(start,startlen),std::string(end,endlen),limit_cpp,use_snapshot);
		return it;		
	}

	nemo_KIteratorRO_t  * nemo_KScanWithHandle(nemo_t *nemo, nemo_DBNemo_t * db,const char * start,const size_t startlen, const char * end, const size_t endlen, bool use_snapshot){
		nemo_KIteratorRO_t * it = new nemo_KIteratorRO_t;
		it->rep = nemo->rep->KScanWithHandle(db->rep,std::string(start,startlen),std::string(end,endlen),1LL << 60,use_snapshot);
		return it;		
	}

	void KRONext(nemo_KIteratorRO_t * it)
	{
		it->rep->Next();
	}
	bool KROValid(nemo_KIteratorRO_t * it){
		return it->rep->Valid();	
	}
	const char* KROkey(nemo_KIteratorRO_t * it, size_t* keylen)
	{
		*keylen = it->rep->key().size();
		return it->rep->key().data();
	}
	const char* KROvalue(nemo_KIteratorRO_t * it, size_t* valuelen)
	{
		*valuelen = it->rep->value().size();
		return it->rep->value().data();
	}

	void KROIteratorFree(nemo_KIteratorRO_t * it)
	{
		delete it->rep;
		delete it;
	}

	void nemo_SeekWithHandle(nemo_t *nemo, nemo_DBNemo_t * db,const char * start,const size_t startlen, char ** NextKey, size_t * NextKeylen,char ** NextVal, size_t * NextVallen,char ** errptr){
		std::string start_str,NextKey_str,NextVal_str;
		start_str.assign(start,startlen);
		Status s = nemo->rep->SeekWithHandle(db->rep, start_str,&NextKey_str,&NextVal_str);
		if(s.ok())
		{
			*NextKey = CopyString(NextKey_str);
			*NextKeylen = NextKey_str.size();
			*NextVal = CopyString(NextVal_str);
			*NextVallen = NextVal_str.size();
			*errptr = nullptr;
		}
		else if(s.IsNotFound())
		{
			*NextKey = nullptr;
			*NextKeylen = 0;
			*NextVal = nullptr;
			*NextVallen = 0;			
			*errptr = nullptr;
		}
	}

	void KNext(nemo_KIterator_t * it)
	{
		it->rep->Next();
	}
	bool KValid(nemo_KIterator_t * it){
		return it->rep->Valid();	
	}
	void Kkey(nemo_KIterator_t * it,char ** key ,size_t* keylen)
	{
		*key = CopyString(it->rep->key());
		*keylen = it->rep->key().size();
	}
	void Kvalue(nemo_KIterator_t * it,char ** value ,size_t* valuelen)
	{
		*value = CopyString(it->rep->value());
		*valuelen = it->rep->value().size();
	}

	void KIteratorFree(nemo_KIterator_t * it)
	{
		delete it->rep;
		delete it;
	}


	void nemo_Scan(nemo_t * nemo, int64_t cursor, const char * pattern, const size_t patternlen, int64_t count, \
				           int * key_num, char *** key_list,size_t ** key_list_strlen, int64_t * cursor_ret, char ** errptr){
		std::vector<std::string> keys;
		std::string pattern_str(pattern,patternlen);
		nemo_SaveError(errptr,nemo->rep->Scan(cursor,pattern_str,count,keys,cursor_ret));
		*key_num = keys.size();
		if(*key_num>0){
			*key_list = new char * [*key_num];
			*key_list_strlen = new size_t [*key_num];
			for(int i=0; i<*key_num; i++){
				(*key_list)[i] = CopyString(keys[i]);
				(*key_list_strlen)[i] = keys[i].size(); 
			}
		}
		else
		{
			*key_list = nullptr;
			*key_list_strlen = nullptr;
		}
	}

    // ==============BITMAP=====================	
	void nemo_BitSet(nemo_t * nemo,const char * key, const size_t keylen, const int64_t offset,const int64_t on, int64_t * res, char ** errptr){
		nemo_SaveError(errptr,nemo->rep->BitSet(std::string(key,keylen),offset,on,res));
	}

	void nemo_BitGet(nemo_t * nemo,const char * key,const size_t keylen,const int64_t offset,int64_t * res, char ** errptr){
		nemo_SaveError(errptr,nemo->rep->BitGet(std::string(key,keylen),offset,res));
	}
	void nemo_BitCount(nemo_t * nemo,const char * key,const size_t keylen,int64_t * res, char ** errptr){
		nemo_SaveError(errptr,nemo->rep->BitCount(std::string(key,keylen),res));
	}
	void nemo_BitCountRange(nemo_t * nemo,const char * key,const size_t keylen,int64_t start,int64_t end,int64_t * res, char ** errptr){
		nemo_SaveError(errptr,nemo->rep->BitCount(std::string(key,keylen),start,end,res));
	}
	void nemo_BitPos(nemo_t * nemo,const char * key,const size_t keylen,const int64_t bit,int64_t * res, char ** errptr){
		nemo_SaveError(errptr,nemo->rep->BitPos(std::string(key,keylen),bit,res));
	}
	void nemo_BitPosWithStart(nemo_t * nemo,const char * key,const size_t keylen,const int64_t bit,const int64_t start,int64_t * res, char ** errptr){
		nemo_SaveError(errptr,nemo->rep->BitPos(std::string(key,keylen),bit,start,res));
	}
	void nemo_BitPosWithStartEnd(nemo_t * nemo,const char * key,const size_t keylen,const int64_t bit,\
									const int64_t start,const int64_t end,int64_t * res, char ** errptr){
		nemo_SaveError(errptr,nemo->rep->BitPos(std::string(key,keylen),bit,start,end,res));
	}
	void nemo_BitOp(nemo_t * nemo,int optype,const char * dest_key,const size_t dest_keylen,const int num, char ** src_key_list, size_t * src_key_len,int64_t * result_length, char ** errptr){
		std::vector<std::string> src_keys(num);
		for (int i = 0; i < num; ++i)
		{
			src_keys[i] = std::string(src_key_list[i],src_key_len[i]);
			/* code */
		}
		nemo_SaveError(errptr,nemo->rep->BitOp(static_cast<BitOpType>(optype),std::string(dest_key,dest_keylen),src_keys,result_length));
	}	

	// ==============HASH=====================
	void nemo_HSet(nemo_t * nemo,const char * key,const size_t keylen,
							     const char * field,const size_t fieldlen,const char * value,const size_t vallen, int * res, char ** errptr){
		nemo_SaveError(errptr,nemo->rep->HSet(rocksdb::Slice(key,keylen),std::string(field,fieldlen),std::string(value,vallen),res));
	}
	void nemo_HGet(nemo_t * nemo,const char * key,const size_t keylen,const char * field,const size_t fieldlen,char ** value, size_t * value_len , char ** errptr){
		std::string val_str;
		Status s = nemo->rep->HGet(rocksdb::Slice(key,keylen),rocksdb::Slice(field,fieldlen),&val_str);
		if(s.ok())
		{
			*errptr = nullptr;
			*value = CopyString(val_str);
			*value_len = val_str.size();
		}
		else if (s.IsNotFound())
		{
			*errptr = nullptr;
			*value  = nullptr ;
			*value_len  = 0;			
		}
		else
		{
			*errptr = strdup(s.ToString().c_str());
		}

	}
	void nemo_HDel(nemo_t * nemo,const char * key,const size_t keylen,const char * field,const size_t fieldlen, char ** errptr){
		nemo_SaveError(errptr,nemo->rep->HDel(rocksdb::Slice(key,keylen),std::string(field,fieldlen)));
	}
	void nemo_HMDel(nemo_t * nemo,const char * key,const size_t keylen,int num,const char ** fieldlist,const size_t * fieldlen, int64_t * res,char ** errptr){
		std::vector<std::string>fields(num);
		for(int i = 0;i<num;i++)
		{	
			fields[i].assign(fieldlist[i],fieldlen[i]);
		}
		nemo_SaveError(errptr,nemo->rep->HMDel(std::string(key,keylen),fields,res));
	}
	void nemo_HExists(nemo_t * nemo,const char * key,const size_t keylen,const char * field,const size_t fieldlen, bool * ifExists,char ** errptr){
		Status s = nemo->rep->HExists(std::string(key,keylen),std::string(field,fieldlen),ifExists);
		if(s.ok()|| s.IsNotFound())
		{
			*errptr = nullptr;
		}
		else
		{
			*errptr = strdup(s.ToString().c_str());
		}
	}
	void nemo_HKeys(nemo_t * nemo,const char * key,const size_t keylen, int* count, char *** field_list, size_t ** field_list_strlen, char ** errptr){
		std::vector<std::string> fields;
		nemo_SaveError(errptr,nemo->rep->HKeys(std::string(key,keylen),fields));
		*count = fields.size();
		if(*count>0){
			*field_list = new char * [*count];
			*field_list_strlen = new size_t [*count];
			for (int i = 0; i < *count; ++i)
			{
				(*field_list)[i] = CopyString( fields[i]); 
				(*field_list_strlen)[i] = fields[i].size(); 
			}
		}
		else{
			*field_list = nullptr;
			*field_list_strlen = nullptr;
		}
	} 	

	void nemo_HGetall(nemo_t * nemo,const char * key,const size_t keylen, int * count, char *** field_list, size_t ** field_list_strlen, \
									  char *** value_list, size_t ** value_list_strlen, char ** errptr){
		std::vector<FV> fv;
		nemo_SaveError(errptr,nemo->rep->HGetall(std::string(key,keylen),fv));
		*count = fv.size();
		if(*count>0){
			*field_list = new char * [*count];
			*value_list = new char * [*count];
			*field_list_strlen = new size_t [*count];
			*value_list_strlen = new size_t [*count];
			for (int i = 0; i < *count; ++i)
			{
				(*field_list)[i] = CopyString( fv[i].field); 
				(*value_list)[i] = CopyString( fv[i].val);
				(*field_list_strlen)[i] =  fv[i].field.size(); 
				(*value_list_strlen)[i] =  fv[i].val.size();
			}
		}
		else{
			*field_list = nullptr;
			*value_list = nullptr;
			*field_list_strlen = nullptr;
			*value_list_strlen = nullptr;
		}
	}

	void nemo_HLen(nemo_t * nemo,const char * key,const size_t keylen,int64_t * len,char ** errptr){
		nemo_SaveError(errptr,nemo->rep->HLen(std::string(key,keylen),len));
	}

	void nemo_HMSet(nemo_t * nemo,const char * key,const size_t keylen, const int num, const char ** field_list,const size_t * field_list_len,	\
								       const char ** value_list,const size_t * value_list_len, int * res_list, char ** errptr){
		std::vector<FVSlice> fv(num);
		for (int i = 0; i < num; ++i)
		{
			fv[i].field = rocksdb::Slice(field_list[i],field_list_len[i]);
			fv[i].val = rocksdb::Slice(value_list[i],value_list_len[i]);/* code */
		}
		nemo_SaveError(errptr,nemo->rep->HMSetSlice(rocksdb::Slice(key,keylen),fv,res_list));
	}
	void * nemo_HMGet(nemo_t * nemo,const char * key,const size_t keylen, const int num,		\
									 const char ** field_list,const size_t * field_list_len,	\
					 			     const char ** value_list,size_t * value_list_strlen, char ** errs,char ** errptr){
		std::vector<SS> *ss = new std::vector<SS>(num);
		std::vector<rocksdb::Slice> keys(num);
		for (int i = 0; i < num; ++i)
		{
			keys[i] = rocksdb::Slice(field_list[i],field_list_len[i]);/* code */
		}
		nemo_SaveError(errptr,nemo->rep->HMGetSlice(rocksdb::Slice(key,keylen),keys,*ss));
		for (int i = 0; i < num; ++i)
		{
			if((*ss)[i].status.ok()){
				value_list[i] = (*ss)[i].val->data();
				value_list_strlen[i] = (*ss)[i].val->size(); 
				errs[i] = nullptr;				
			}
			else if((*ss)[i].status.IsNotFound()){
				value_list[i] = nullptr;
				value_list_strlen[i] = 0; 
				errs[i] = nullptr;					
			}
			else{
				errs[i] = strdup((*ss)[i].status.ToString().c_str());
			}
		}
		return (void *) ss;
	}

	void nemo_HSetnx(nemo_t * nemo,const char * key,const size_t keylen,const char * field,const size_t fieldlen,const char * value,const size_t vallen, int64_t * res, char ** errptr){
		nemo_SaveError(errptr,nemo->rep->HSetnx(std::string(key,keylen),std::string(field,fieldlen),std::string(value,vallen),res));
	}

	void nemo_HStrlen(nemo_t * nemo,const char * key,const size_t keylen,const char * field,const size_t fieldlen,int64_t * res_len ,char **errptr){
		 Status s = nemo->rep->HStrlen(std::string(key,keylen),std::string(field,fieldlen),res_len);
		if(s.ok()|| s.IsNotFound())
		{
			*errptr = nullptr;
		}
		else
		{
			*errptr = strdup(s.ToString().c_str());
		}		 
	}

//  HIterator* HScan(const std::string &key, const std::string &start, const std::string &end, uint64_t limit, bool use_snapshot = false);
	void nemo_HVals(nemo_t * nemo,const char * key,const size_t keylen,char *** val_list, size_t ** val_list_strlen, int * count, char ** errptr){
		std::vector<std::string> vals;
		nemo_SaveError(errptr,nemo->rep->HVals(std::string(key,keylen),vals));
		*count = vals.size();
		if(*count>0){
			*val_list = new char * [*count];
			*val_list_strlen = new size_t [*count];
			for (int i = 0; i < *count; ++i)
			{
				(*val_list)[i] = CopyString( vals[i]); 
				(*val_list_strlen)[i] =  vals[i].size(); 
			}
		}
		else{
			*val_list = nullptr;
			*val_list_strlen = nullptr;
		}
	} 	
	void nemo_HIncrby(nemo_t * nemo,const char * key,const size_t keylen,const char * field,const size_t fieldlen,const int64_t by, char ** new_val, size_t * new_val_len, char ** errptr){
		std::string new_val_cpp;
		nemo_SaveError(errptr,nemo->rep->HIncrby(std::string(key,keylen),std::string(field,fieldlen),by,new_val_cpp));
		*new_val = CopyString(new_val_cpp); 
		*new_val_len = new_val_cpp.size();
	}
	void nemo_HIncrbyfloat(nemo_t * nemo,const char * key,const size_t keylen,const char * field,const size_t fieldlen,const double by, char ** new_val, size_t * new_val_len, char ** errptr){
		std::string new_val_cpp;
		nemo_SaveError(errptr,nemo->rep->HIncrbyfloat(std::string(key,keylen),std::string(field,fieldlen),by,new_val_cpp));
		*new_val = CopyString(new_val_cpp);
		*new_val_len = new_val_cpp.size(); 
	}

	void * nemo_HGetIndexInfo(nemo_t * nemo,const char * key,const size_t keylen, const char ** index, size_t * index_len, int32_t * res, char ** errptr){
		std::string * val_str;
		Status s = nemo->rep->HGetIndexInfo(rocksdb::Slice(key,keylen),&val_str);
		if(s.ok())
		{
			int64_t len = sizeof(int64_t)*2;
			*errptr = nullptr;
			if(val_str->size()>len){
				*index = val_str->data()+len;
				*index_len = val_str->size()-len;
				*res = 0;
			}
			else {
				*index = nullptr;
				*index_len = 0;
				*res = 0;
			}
		}
		else if(s.IsNotFound()){
			*index = nullptr;
			*index_len = 0;
			*res = -1;
		}
		else
		{
			*errptr = strdup(s.ToString().c_str());
		}
		return (void * ) val_str;
	}
	void nemo_HSetIndexInfo(nemo_t * nemo,const char * key,const size_t keylen, const char * index, size_t index_len , char ** errptr){
		nemo_SaveError(errptr,nemo->rep->HSetIndexInfo(rocksdb::Slice(key,keylen),rocksdb::Slice(index,index_len)));
	}

	nemo_HmetaIterator_t  * nemo_HmetaScan(nemo_t *nemo, 			\
						const char * start,const size_t startlen, const char * end, const size_t endlen, 	\
						bool use_snapshot, bool skip_nil_index){
		nemo_HmetaIterator_t * it = new nemo_HmetaIterator_t;
		it->rep = nemo->rep->HmetaScan(std::string(start,startlen),std::string(end,endlen),1LL << 60,use_snapshot,skip_nil_index);
		if(it->rep->Valid()){
			if(skip_nil_index) {
				while(1){
					if(it->rep->IndexInfo().size()>0)
						break;
					else if(!it->rep->Valid())
						break;
					it->rep->Next();
				}
			}
		}
		return it;
	}

	void HmetaNext(nemo_HmetaIterator_t * it)
	{
		if(it->rep->_skip_nil_index){
			while(1){
				it->rep->Next();
				if(it->rep->IndexInfo().size()>0)
					break;
				else if(!it->rep->Valid())
					break;
			}
		} else {
			it->rep->Next();
		}
	}
	
	bool HmetaValid(nemo_HmetaIterator_t * it){
		return it->rep->Valid();	
	}
	const char* HmetaKey(nemo_HmetaIterator_t * it, size_t* keylen)
	{
		*keylen = it->rep->key().size();
		return it->rep->key().data();
	}
	const char* HmetaIndexInfo(nemo_HmetaIterator_t * it, size_t* indexlen)
	{
		*indexlen = it->rep->IndexInfo().size();
		return it->rep->IndexInfo().data();
	}
	void HmetaIteratorFree(nemo_HmetaIterator_t * it)
	{
		delete it->rep;
		delete it;
	}

	nemo_HIterator_t  * nemo_HScan(nemo_t *nemo, 			\
						const char * key, const size_t keylen,	\
						const char * start,const size_t startlen, \
						const char * end, const size_t endlen, 	\
						bool use_snapshot) {
		nemo_HIterator_t * it = new nemo_HIterator_t;
		it->rep = nemo->rep->HScan(std::string(key,keylen),std::string(start,startlen),std::string(end,endlen),1LL << 60,use_snapshot);
		return it;
	}
	void HNext(nemo_HIterator_t * it)
	{
		it->rep->Next();
	}
	bool HValid(nemo_HIterator_t * it){
		return it->rep->Valid();	
	}
	const char* HKey(nemo_HIterator_t * it, size_t* keylen)
	{
		*keylen = it->rep->key().size();
		return it->rep->key().data();
	}	
	const char* HField(nemo_HIterator_t * it, size_t* filedlen)
	{
		*filedlen = it->rep->field().size();
		return it->rep->field().data();
	}
	const char* HValue(nemo_HIterator_t * it, size_t* vallen)
	{
		*vallen = it->rep->value().size();
		return it->rep->value().data();
	}
	void HIteratorFree(nemo_HIterator_t * it)
	{
		delete it->rep;
		delete it;
	}

	// ==============List=====================
	void nemo_LIndex(nemo_t * nemo,const char * key, const size_t keylen, const int64_t index, char ** val, size_t * val_len, char ** errptr){
		std::string val_str;
		Status s = nemo->rep->LIndex(std::string(key,keylen),index,&val_str);
		if(s.ok())
		{
			*errptr = nullptr;
			*val = CopyString(val_str);
			*val_len = val_str.size();
		}
		else if (s.IsNotFound()){
			*errptr = nullptr;
			*val = nullptr;
			*val_len = 0; 
		}
		else
		{
			*errptr = strdup(s.ToString().c_str());
		}
	}
	void nemo_LLen(nemo_t * nemo,const char * key,const size_t keylen, int64_t * llen,char ** errptr){
		std::string val_str;
		Status s = nemo->rep->LLen(std::string(key,keylen),llen);
		if(s.ok()|| s.IsNotFound())
		{
			*errptr = nullptr;
		}
		else
		{
			*errptr = strdup(s.ToString().c_str());
		}
	}
	void nemo_LPush(nemo_t * nemo,const char * key,const size_t keylen,const char * value,const size_t vallen, int64_t * llen,char ** errptr){
		nemo_SaveError(errptr,nemo->rep->LPush(std::string(key,keylen),std::string(value,vallen),llen));
	}
	void nemo_LPop(nemo_t * nemo,const char * key,const size_t keylen,char ** value, size_t * value_len, char ** errptr){
		std::string val_str;
		Status s = nemo->rep->LPop(std::string(key,keylen),&val_str);
		if(s.ok())
		{
			*errptr = nullptr;
			*value = CopyString(val_str);
			*value_len = val_str.size();
		}
		else if(s.IsNotFound())
		{
			*errptr = nullptr;
			*value = nullptr;
			*value_len = 0;			
		}
		else
		{
			*errptr = strdup(s.ToString().c_str());
		}
	}    	
	void nemo_LPushx(nemo_t * nemo,const char * key,const size_t keylen,const char * value, const size_t vallen,int64_t * llen,char ** errptr){
		Status s = nemo->rep->LPushx(std::string(key,keylen),std::string(value,vallen),llen);
		if(s.ok()|| s.IsNotFound())
		{
			*errptr = nullptr;
		}
		else
		{
			*errptr = strdup(s.ToString().c_str());
		}
	}
	void nemo_LRange(nemo_t * nemo,const char * key,const size_t keylen,const int64_t begin,const int64_t end, \
					size_t * num, int64_t ** index_list,char *** val_list, size_t ** val_list_strlen, int64_t * res, char ** errptr){
		std::vector<IV> ivs;
		Status s = nemo->rep->LRange(std::string(key,keylen),begin,end,ivs);

		if(s.ok())
		{
			*errptr = nullptr;
			*res = 1;
		}
		else if (s.IsNotFound())
		{
			*errptr = nullptr;
			*res = 0;		
		}
		else
		{
			*errptr = strdup(s.ToString().c_str());
		}		
		
		*num = ivs.size();
		if(*num>0){
			*index_list = new int64_t [*num] ;
			*val_list = new char * [*num];
			*val_list_strlen = new size_t [*num];			
			for (size_t i = 0; i < *num ; ++i)
			{
				(*index_list)[i] = ivs[i].index;
				(*val_list)[i] = CopyString(ivs[i].val);
				(*val_list_strlen)[i] = ivs[i].val.size();
				/* code */
			}
		}
		else{
			*index_list = nullptr;
			*val_list = nullptr;
			*val_list_strlen = nullptr;	
		}
	} 

	void nemo_LSet(nemo_t * nemo,const char * key,const size_t keylen, const int64_t index, char * val,const size_t vallen,char ** errptr){
		nemo_SaveError(errptr,nemo->rep->LSet(std::string(key,keylen),index,std::string(val,vallen)));
	}
	void nemo_LTrim(nemo_t * nemo,const char * key,const size_t keylen, const int64_t begin, const int64_t end,char ** errptr){
		Status s = nemo->rep->LTrim(std::string(key,keylen),begin,end);
		if(s.ok()|| s.IsNotFound())
		{
			*errptr = nullptr;
		}
		else
		{
			*errptr = strdup(s.ToString().c_str());
		}		
	}
	void nemo_RPush(nemo_t * nemo,const char * key,const size_t keylen,const char * value,const size_t vallen, int64_t * llen,char ** errptr){
		nemo_SaveError(errptr,nemo->rep->RPush(std::string(key,keylen),std::string(value,vallen),llen));
	}
	void nemo_RPop(nemo_t * nemo,const char * key,const size_t keylen,char ** value, size_t * value_len, char ** errptr){
		std::string val_str;
		Status s = nemo->rep->RPop(std::string(key,keylen),&val_str);

		if(s.ok())
		{
			*errptr = nullptr;
			*value = CopyString(val_str);
			*value_len = val_str.size();
		}
		else if(s.IsNotFound())
		{
			*errptr = nullptr;
			*value = nullptr;
			*value_len = 0;			
		}
		else
		{
			*errptr = strdup(s.ToString().c_str());
		}

	}
	void nemo_RPushx(nemo_t * nemo,const char * key,const size_t keylen,const char * value,const size_t vallen, int64_t * llen,char ** errptr){
		Status s = nemo->rep->RPushx(std::string(key,keylen),std::string(value,vallen),llen);
		if(s.ok()|| s.IsNotFound())
		{
			*errptr = nullptr;
		}
		else
		{
			*errptr = strdup(s.ToString().c_str());
		}
	}
	void nemo_RPopLPush(nemo_t * nemo,const char * src,const size_t srclen,char * dest,const size_t destlen,char **val, size_t * val_len, int64_t * res, char ** errptr){
		std::string val_str;
		Status s = nemo->rep->RPopLPush(std::string(src,srclen),std::string(dest,destlen),val_str);

		if(s.ok())
		{
			*errptr = nullptr;
			*res = 1;
			*val = CopyString(val_str);
			*val_len = val_str.size();
		}
		else if (s.IsNotFound())
		{
			*errptr = nullptr;
			*res = 0;
			*val = nullptr;
			*val_len = 0;			
		}
		else
		{
			*errptr = strdup(s.ToString().c_str());
		}

	}   
	void nemo_LInsert(nemo_t * nemo,const char * key,const size_t keylen, int pos,\
								    const char * pivot,const size_t pivotlen,\
									const char * val,const size_t vallen,int64_t *llen,char ** errptr){
		Status s = nemo->rep->LInsert(std::string(key,keylen),static_cast<Position>(pos),std::string(pivot,pivotlen),std::string(val,vallen),llen);
		if(s.ok()|| s.IsNotFound())
		{
			*errptr = nullptr;
		}
		else
		{
			*errptr = strdup(s.ToString().c_str());
		}		
	}
	void nemo_LRem(nemo_t * nemo,const char * key,const size_t keylen, const int64_t count, const char * val, const size_t vallen,int64_t * rem_count,char ** errptr){
		Status s = nemo->rep->LRem(std::string(key,keylen),count,std::string(val,vallen),rem_count);
		if(s.ok()|| s.IsNotFound())
		{
			*errptr = nullptr;
		}
		else
		{
			*errptr = strdup(s.ToString().c_str());
		}
	}
	// ==============Set=====================
	void nemo_SAdd(nemo_t * nemo,const char * key,const size_t keylen, const char * member,const size_t memberlen,int64_t *res,char ** errptr){
		nemo_SaveError(errptr,nemo->rep->SAdd(std::string(key,keylen),std::string(member,memberlen),res));
	}	
	void nemo_SMAdd(nemo_t * nemo,const char * key,const size_t keylen,const int num, const char ** member,const size_t * memberlen,int64_t *res,char ** errptr){
		std::vector<std::string> members(num);
		for(int i = 0;i<num;i++)
		{
			members[i].assign(member[i],memberlen[i]);
		}			
		nemo_SaveError(errptr,nemo->rep->SMAdd(std::string(key,keylen),members,res));
	}	
	void nemo_SRem(nemo_t * nemo,const char * key,const size_t keylen, const char * member,const size_t memberlen,int64_t *res,char ** errptr){
		nemo_SaveError(errptr,nemo->rep->SRem(std::string(key,keylen),std::string(member,memberlen),res));
	}
	void nemo_SMRem(nemo_t * nemo,const char * key,const size_t keylen,const int num, const char ** member,const size_t * memberlen,int64_t *res,char ** errptr){
		std::vector<std::string> members(num);
		for(int i = 0;i<num;i++)
		{
			members[i].assign(member[i],memberlen[i]);
		}			
		nemo_SaveError(errptr,nemo->rep->SMRem(std::string(key,keylen),members,res));
	}
	void nemo_SCard(nemo_t * nemo,const char * key,const size_t keylen,int64_t * sum, char ** errptr ){
		Status s = nemo->rep->SCard(std::string(key,keylen),sum) ;
		if(s.ok()|| s.IsNotFound())
		{
			*errptr = nullptr;
		}
		else
		{
			*errptr = strdup(s.ToString().c_str());
		}		
	}
	void nemo_SMembers(nemo_t * nemo,const char * key,const size_t keylen,char *** member_list, size_t ** member_list_strlen,int * count, char ** errptr){
		std::vector<std::string> vals;
		nemo_SaveError(errptr,nemo->rep->SMembers(std::string(key,keylen),vals));
		*count = vals.size();
		if(*count>0){
			*member_list = new char * [*count];
			*member_list_strlen = new size_t [*count];
			for (int i = 0; i < *count; ++i)
			{
				(*member_list)[i] = CopyString( vals[i]); 
				(*member_list_strlen)[i] =  vals[i].size(); 
			}
		}
		else{
			*member_list = nullptr;
			*member_list_strlen = nullptr;
		}
	} 
	void nemo_SUnionStore(nemo_t * nemo,const char * destination, const size_t destlen,\
							const int num, const char ** key_list,const size_t * key_list_len,	\
																  int64_t * res, char ** errptr){
		int64_t res_cpp;
		std::vector<std::string> keys(num);
		for (int i = 0; i < num; ++i)
		{
			keys[i] = std::string(key_list[i],key_list_len[i]);
		}
		nemo_SaveError(errptr,nemo->rep->SUnionStore(std::string(destination,destlen),keys,&res_cpp));
		*res = res_cpp;
	}

	void nemo_SUnion(nemo_t * nemo, const int num, const char ** key_list,const size_t * key_list_len,		\
				         int * count,char *** val_list, size_t ** val_list_strlen, char ** errptr){
		std::vector<std::string> keys(num);
		std::vector<std::string> vals;
		for (int i = 0; i < num; ++i)
		{
			keys[i] = std::string(key_list[i],key_list_len[i]);
		}
		nemo_SaveError(errptr,nemo->rep->SUnion(keys,vals));
		*count = vals.size();
		if(*count>0){
			*val_list = new char * [*count];
			*val_list_strlen = new size_t [*count];
			for (int i = 0; i < *count; ++i)
			{
				(*val_list)[i] = CopyString(vals[i]);
				(*val_list_strlen)[i] = vals[i].size();
			}
		}
		else{
			*val_list = nullptr;
			*val_list_strlen = nullptr;
		}
	}

	void nemo_SInterStore(nemo_t * nemo,const char * destination, const size_t destlen , \
						const int num, const char ** key_list,const size_t * key_list_len,	\
																  int64_t * res, char ** errptr){
		std::vector<std::string> keys(num);
		for (int i = 0; i < num; ++i)
		{
			keys[i] = std::string(key_list[i],key_list_len[i]);
		}
		nemo_SaveError(errptr,nemo->rep->SInterStore(std::string(destination,destlen),keys,res));
	}

	void nemo_SInter(nemo_t * nemo, const int num, const char ** key_list,const size_t * key_list_len,		\
					int * count,char *** val_list, size_t ** val_list_strlen, char ** errptr){
		std::vector<std::string> keys(num);
		std::vector<std::string> vals;
		for (int i = 0; i < num; ++i)
		{
			keys[i] = std::string(key_list[i],key_list_len[i]);
		}
		nemo_SaveError(errptr,nemo->rep->SInter(keys,vals));
		*count = vals.size();
		if(*count >0){
			*val_list = new char * [*count];
			*val_list_strlen = new size_t [*count];
			for (int i = 0; i < *count; ++i)
			{
				(*val_list)[i] = CopyString(vals[i]); 
				(*val_list_strlen)[i] = vals[i].size(); 
			}
		}
		else{
			*val_list = nullptr;
			*val_list_strlen = nullptr;
		}
	}

	void nemo_SDiffStore(nemo_t * nemo,const char * destination,const size_t destlen ,const int num, const char ** key_list,const size_t * key_list_len,		\
																  int64_t * res, char ** errptr){
		std::vector<std::string> keys(num);
		for (int i = 0; i < num; ++i)
		{
			keys[i] = std::string(key_list[i],key_list_len[i]);
		}
		nemo_SaveError(errptr,nemo->rep->SDiffStore(std::string(destination,destlen),keys,res));
	}	

	void nemo_SDiff(nemo_t * nemo, const int num, const char ** key_list,const size_t * key_list_len,		\
									int * count,char *** val_list, size_t ** val_list_strlen, char ** errptr){
		std::vector<std::string> keys(num);
		std::vector<std::string> vals;
		for (int i = 0; i < num; ++i)
		{
			keys[i] = std::string(key_list[i],key_list_len[i]);
		}
		nemo_SaveError(errptr,nemo->rep->SDiff(keys,vals));
		*count = vals.size();
		if(*count > 0){
			*val_list = new char * [*count];
			*val_list_strlen = new size_t [*count];
			for (int i = 0; i < *count; ++i)
			{
				(*val_list)[i] = CopyString(vals[i]); 
				(*val_list_strlen)[i] = vals[i].size(); 
			}
		}
		else{
			*val_list = nullptr;
			*val_list_strlen = nullptr;
		}

	}	
	void nemo_SIsMember(nemo_t * nemo,const char * key,const size_t keylen, const char * member,const size_t memlen,bool * isMember,char ** errptr){
		Status s = nemo->rep->SIsMember(std::string(key,keylen),std::string(member,memlen),isMember);
		if(s.ok()|| s.IsNotFound())
		{
			*errptr = nullptr;
		}
		else
		{
			*errptr = strdup(s.ToString().c_str());
		}			
	}

	void nemo_SPop(nemo_t * nemo,const char * key,const size_t keylen,  char ** member, size_t * len, int64_t * res, char ** errptr){
		std::string member_str;
		Status s = nemo->rep->SPop(std::string(key,keylen),member_str);

		if(s.ok())
		{
			*errptr = nullptr;
			*res = 1;
			*member = CopyString(member_str);
			*len = member_str.size();			
		}
		else if (s.IsNotFound())
		{
			*errptr = nullptr;
			*res = 0;
			*member = nullptr;
			*len = 0;			
		}
		else
		{
			*errptr = strdup(s.ToString().c_str());
		}		
	}

	void nemo_SRandomMember(nemo_t * nemo,  const char * key,const size_t keylen,		\
						int * res_count,char *** member_list, size_t ** member_list_strlen, const int count, int64_t * res, char ** errptr){
		std::vector<std::string> members(count);
		Status s = nemo->rep->SRandMember(std::string(key,keylen),members,count);
		if(s.ok())
		{
			*errptr = nullptr;
			*res = 1;
		}
		else if (s.IsNotFound())
		{
			*errptr = nullptr;
			*res = 0;
		}
		else
		{
			*errptr = strdup(s.ToString().c_str());
		}		
		*res_count = members.size();
		if (*res_count > 0)
		{
			*member_list = new char * [*res_count];
			*member_list_strlen = new size_t [*res_count];
			for (int i = 0; i < *res_count; ++i)
			{
				(*member_list)[i] = CopyString(members[i]); 
				(*member_list_strlen)[i] = members[i].size();
			}			
		}
		else
		{
			*member_list = nullptr;
			*member_list_strlen = nullptr;
		}

	}
    void nemo_SMove(nemo_t * nemo,const char * source,const size_t slen,const char * dest,const size_t destlen,\
						const char * member,const size_t memlen,int64_t * res,char ** errptr){
    	Status s = nemo->rep->SMove(std::string(source,slen),std::string(dest,destlen),std::string(member,memlen),res);
		if(s.ok()|| s.IsNotFound())
		{
			*errptr = nullptr;
		}
		else
		{
			*errptr = strdup(s.ToString().c_str());
		}
    }

    // ==============ZSet=====================
	void nemo_ZAdd(nemo_t * nemo,const char * key, const size_t keylen, const double score, const char * member,const size_t memlen,int64_t *res,char ** errptr){
		nemo_SaveError(errptr,nemo->rep->ZAdd(std::string(key,keylen),score,std::string(member,memlen),res));
	}
	void nemo_ZMAdd(nemo_t * nemo,const char * key, const size_t keylen, const int num, const double *score, const char ** member,const size_t *memlen,int64_t *res,char ** errptr){
		std::vector<SM> sms(num);
		for(int i=0;i<num;i++)
		{
			sms[i].member.assign(member[i],memlen[i]);
			sms[i].score = score[i];
		}
		nemo_SaveError(errptr,nemo->rep->ZMAdd(std::string(key,keylen),sms,res));
	}
	void nemo_ZCard(nemo_t * nemo,const char * key,const size_t keylen,int64_t * sum, char ** errptr){
		Status s  = nemo->rep->ZCard(std::string(key,keylen),sum);
		if(s.ok()|| s.IsNotFound())
		{
			*errptr = nullptr;
		}
		else
		{
			*errptr = strdup(s.ToString().c_str());
		}
	}
    void nemo_ZCount(nemo_t * nemo,const char * key,const size_t keylen,const double begin,const double end ,int64_t* sum, bool is_lo,bool is_ro,char ** errptr){
    	nemo_SaveError(errptr,nemo->rep->ZCount(std::string(key,keylen),begin,end,sum,is_lo,is_ro));
    }
//  ZIterator* ZScan(const std::string &key, const double begin, const double end, uint64_t limit, bool use_snapshot = false);    
	void nemo_ZIncrby(nemo_t * nemo,const char * key,const size_t keylen, \
				const char * member,const size_t memlen,const double by, char ** new_val,size_t * new_val_len,char ** errptr){
		std::string new_val_str;
		nemo_SaveError(errptr,nemo->rep->ZIncrby(std::string(key,keylen),std::string(member,memlen),by,new_val_str));
		*new_val = CopyString(new_val_str);
		*new_val_len = new_val_str.size();
	}

    void nemo_ZRange(nemo_t * nemo,const char * key,const size_t keylen,const int64_t start,const int64_t stop,\
    						size_t * num,double ** score_list,char *** member_list,size_t ** member_list_strlen,char ** errptr){
    	std::vector<SM> sms;
    	nemo_SaveError(errptr,nemo->rep->ZRange(std::string(key,keylen),start,stop,sms));
		*num = sms.size();
		if(*num>0)
		{
			*score_list				= new double[*num];
			*member_list			= new char *[*num];
			*member_list_strlen		= new size_t[*num];
	    	for(size_t i = 0 ;i< *num; i++)
			{
				(*score_list)[i] = sms[i].score;
				(*member_list)[i] = CopyString( sms[i].member);
				(*member_list_strlen)[i] = sms[i].member.size();
			}
		}else
		{
			*score_list				= nullptr;
			*member_list			= nullptr;
			*member_list_strlen		= nullptr;			
		}
    }

 	void nemo_ZUnionStore(nemo_t * nemo,const char * destination, const size_t destlen,const int numkeys, 	\
						  const int list_len,const char ** key_list,const size_t * key_list_len,const double * weight_list,	\
						  int agg,int64_t * res, char ** errptr){
		std::vector<std::string> keys(list_len);
		std::vector<double> weights(list_len);
		for (int i = 0; i < list_len; ++i)
		{
			keys[i] = std::string(key_list[i],key_list_len[i]);
			weights[i] = weight_list[i];
		}
		nemo_SaveError(errptr,nemo->rep->ZUnionStore(std::string(destination,destlen),numkeys,keys,weights,static_cast<Aggregate>(agg),res));
	}

 	void nemo_ZInterStore(nemo_t * nemo,const char * destination,const size_t destlen, const int numkeys, 	\
						  const int list_len,const char ** key_list,const size_t * key_list_len,const double * weight_list,	\
						  int agg,int64_t * res, char ** errptr){
		std::vector<std::string> keys(list_len);
		std::vector<double> weights(list_len);
		for (int i = 0; i < list_len; ++i)
		{
			keys[i] = std::string(key_list[i],key_list_len[i]);
			weights[i] = weight_list[i];
		}
		nemo_SaveError(errptr,nemo->rep->ZInterStore(std::string(destination,destlen),numkeys,keys,weights,static_cast<Aggregate>(agg),res));
	}
    void nemo_ZRangebyScore(nemo_t * nemo,const char * key,const size_t keylen,const double mn,const double mx,\
    						int * count,double ** score_list,char *** member_list, size_t ** member_list_strlen,\
							bool is_lo, bool is_ro, char ** errptr){
    	std::vector<SM> sms;
    	nemo_SaveError(errptr,nemo->rep->ZRangebyscore(std::string(key,keylen),mn,mx,sms,is_lo,is_ro));
    	*count = sms.size();
		if(*count>0){
				*score_list = new double [*count];
				*member_list = new char * [*count];
				*member_list_strlen = new size_t [*count];
				for(int i = 0 ;i< *count;i++)
				{
					(*score_list)[i] = sms[i].score;
					(*member_list)[i] = CopyString( sms[i].member);
					(*member_list_strlen)[i] = sms[i].member.size();
				}
		}
		else{
			*score_list = nullptr;
			*member_list = nullptr;
			*member_list_strlen = nullptr;
		}
    }  
	void nemo_ZRem(nemo_t * nemo,const char * key,const size_t keylen, const char * member,const size_t memlen,int64_t *res,char ** errptr){
		nemo_SaveError(errptr,nemo->rep->ZRem(std::string(key,keylen),std::string(member,memlen),res));
	}
	void nemo_ZMRem(nemo_t * nemo,const char * key,const size_t keylen,const int num, const char ** member,const size_t * memlen,int64_t *res,char ** errptr){
		std::vector<std::string> members(num);
		for(int i = 0;i<num;i++)
		{
			members[i].assign(member[i],memlen[i]);
		}		
		nemo_SaveError(errptr,nemo->rep->ZMRem(std::string(key,keylen),members,res));
	}	
	void nemo_ZRank(nemo_t * nemo,const char * key,const size_t keylen, const char * member,const size_t memlen,int64_t *rank,char ** errptr){
		Status s = nemo->rep->ZRank(std::string(key,keylen),std::string(member,memlen),rank);
		if(s.ok())
		{
			*errptr = nullptr;
		}
		else if (s.IsNotFound())
		{
			*errptr = nullptr;
			*rank = -1;		
		}
		else
		{
			*errptr = strdup(s.ToString().c_str());
		}		
	}
	void nemo_ZRevrank(nemo_t * nemo,const char * key, const size_t keylen, const char * member,const size_t memlen,int64_t *rank,char ** errptr){
		Status s = nemo->rep->ZRevrank(std::string(key,keylen),std::string(member,memlen),rank);
		if(s.ok())
		{
			*errptr = nullptr;
		}
		else if (s.IsNotFound())
		{
			*errptr = nullptr;
			*rank = -1;		
		}
		else
		{
			*errptr = strdup(s.ToString().c_str());
		}			
	}
	void nemo_ZScore(nemo_t * nemo,const char * key, const size_t keylen, const char * member,const size_t memlen, double * score, int64_t * res, char ** errptr){
		
		Status s = nemo->rep->ZScore(std::string(key,keylen),std::string(member,memlen),score);

		if(s.ok())
		{
			*errptr = nullptr;
			*res = 1;
		}
		else if (s.IsNotFound())
		{
			*errptr = nullptr;
			*res = 0;		
		}
		else
		{
			*errptr = strdup(s.ToString().c_str());
		}

	}
    void nemo_ZRangebylex(nemo_t * nemo,const char * key,const size_t keylen,const char * min,const size_t minlen,const char * max, const size_t maxlen, \
				int * num,char *** member_list, size_t ** member_list_strlen, bool is_lo, bool is_ro,char ** errptr){
    	std::vector<std::string> members;
    	nemo_SaveError(errptr,nemo->rep->ZRangebylex(std::string(key,keylen),std::string(min,minlen),std::string(max,maxlen),members,is_lo,is_ro));
    	*num = members.size();
		if(*num>0){
				*member_list = new char * [*num];
				*member_list_strlen = new size_t [*num];
				for(int i = 0 ;i< *num;i++)
				{
					(*member_list)[i] = CopyString( members[i]);
					(*member_list_strlen)[i] = members[i].size();
				}
		}
		else
		{
			*member_list = nullptr;
			*member_list_strlen = nullptr;
		}
	
    }
    void nemo_ZLexcount(nemo_t * nemo,const char * key,const size_t keylen,const char * min,const size_t minlen,const char * max,const size_t maxlen,int64_t * count, bool is_lo, bool is_ro, char ** errptr){
     	nemo_SaveError(errptr,nemo->rep->ZLexcount(std::string(key,keylen),std::string(min,minlen),std::string(max,maxlen),count,is_lo,is_ro));
    }
    void nemo_ZRemrangebylex(nemo_t * nemo,const char * key,const size_t keylen,const char * min,const size_t minlen,const char * max,const size_t maxlen,bool is_lo,bool is_ro,int64_t * count,char ** errptr){
     	nemo_SaveError(errptr,nemo->rep->ZRemrangebylex(std::string(key,keylen),std::string(min,minlen),std::string(max,maxlen),is_lo,is_ro,count));
    } 
    void nemo_ZRemrangebyrank(nemo_t * nemo,const char * key,const size_t keylen,const int64_t start,const int64_t stop,int64_t * count,char ** errptr){
     	nemo_SaveError(errptr,nemo->rep->ZRemrangebyrank(std::string(key,keylen),start,stop,count));
    }            
    void nemo_ZRemrangebyscore(nemo_t * nemo,const char * key,const size_t keylen,const double start,const double stop,int64_t * count,bool is_lo,bool is_ro,char ** errptr){
     	nemo_SaveError(errptr,nemo->rep->ZRemrangebyscore(std::string(key,keylen),start,stop,count,is_lo,is_ro));
    } 
/*
    // ==============HyperLogLog=====================
    void nemo_PfAdd(nemo_t * nemo,const char * key,const size_t keylen,const int num,const char ** value_list,size_t * value_list_len,bool * update,char ** errptr){
    	std::vector<std::string> values(num);
    	for (int i = 0; i < num; ++i)
    	{
    		values[i] = std::string(value_list[i],value_list_len[i]);
    	}
    	nemo_SaveError(errptr,nemo->rep->PfAdd(std::string(key,keylen),values,*update));
    }
    void nemo_PfCount(nemo_t * nemo,const int num,const char ** key_list,const size_t * key_list_len,int *res,char ** errptr){
    	std::vector<std::string> keys(num);
    	int res_cpp;
    	for (int i = 0; i < num; ++i)
    	{
    		keys[i] = std::string(key_list[i],key_list_len[i]);
    	}
    	nemo_SaveError(errptr,nemo->rep->PfCount(keys,res_cpp));
    	*res = res_cpp;
    }
    void nemo_PfMerge(nemo_t * nemo,const int num,const char ** key_list,const size_t * key_list_len,char ** errptr){
    	std::vector<std::string> keys(num);
    	for (int i = 0; i < num; ++i)
    	{
    		keys[i] = std::string(key_list[i],key_list_len[i]);
    	}
    	nemo_SaveError(errptr,nemo->rep->PfMerge(keys));
    }
*/
    // ==============Server=====================
	void nemo_BGSaveGetSnapshot(nemo_t * nemo,int * count,nemo_Snaptshot_t ** snapshot_list,char ** errptr){
		Snapshots snapshots;
		nemo_SaveError(errptr,nemo->rep->BGSaveGetSnapshot(snapshots));
		*count = snapshots.size();
		*snapshot_list = new nemo_Snaptshot_t  [*count];
		for (int i = 0; i < *count; ++i)
		{
			(*snapshot_list)[i].rep =  snapshots[i];
		}
	}
	void nemo_BGSaveSpecify(nemo_t * nemo,const char * key_type,nemo_Snaptshot_t * snapshot,char ** errptr){
		nemo_SaveError(errptr,nemo->rep->BGSaveSpecify(std::string(key_type),snapshot->rep));
	}
	void nemo_BGSaveGetSpecifySnapshot(nemo_t * nemo,const char * key_type,nemo_Snaptshot_t ** snapshot,char ** errptr){
		Snapshot *  snapshot_ref;
		*snapshot = new nemo_Snaptshot_t;
		nemo_SaveError(errptr,nemo->rep->BGSaveGetSpecifySnapshot(std::string(key_type),snapshot_ref));
		(*snapshot)->rep = snapshot_ref;
	}
	void nemo_BGSave(nemo_t * nemo,int len,nemo_Snaptshot_t * snapshot_list,const char * db_path,char ** errptr){
		Snapshots snapshots(len);
		for (int i = 0; i < len; ++i)
		{
			snapshots[i] = snapshot_list[i].rep;
		}
		nemo_SaveError(errptr,nemo->rep->BGSave(snapshots,std::string(db_path)));
	}
	void nemo_BGSaveOff(nemo_t * nemo,char ** errptr){
		nemo_SaveError(errptr,nemo->rep->BGSaveOff());
	}

	void nemo_GetKeyNum(nemo_t * nemo,int * count,long long unsigned int ** num,char ** errptr)
	{
		std::vector<uint64_t> nums;
		nemo_SaveError(errptr,nemo->rep->GetKeyNum(nums));
		*count = nums.size();
		* num = new long long unsigned int  [*count];
		for (int i = 0; i < *count; ++i)
		{
			(*num)[i] = nums[i];/* code */
		}
	}
	void nemo_GetSpecifyKeyNum(nemo_t * nemo,const char * type,long long unsigned int * num,char ** errptr)
	{
		uint64_t num_cpp;
		nemo_SaveError(errptr,nemo->rep->GetSpecifyKeyNum(std::string(type),num_cpp));
		*num = num_cpp;

	}

//    Status ScanKeyNum(std::unique_ptr<rocksdb::DBNemo> &db, const char kType, uint64_t &num);
//    Status ScanKeyNumWithTTL(std::unique_ptr<rocksdb::DBNemo> &db, uint64_t &num);
//	  Status StopScanKeyNum();		

	void nemo_GetUsage(nemo_t * nemo,const char * type,long long unsigned int * res,char ** errptr){
		uint64_t res_cpp;
	 	nemo_SaveError(errptr,nemo->rep->GetUsage(std::string(type),&res_cpp));
	 	*res = res_cpp;
	}

//	 Status ScanMetasSpecify(DBType type, const std::string &pattern,
//        std::map<std::string, MetaPtr>& metas);

	void nemo_CheckMetaSpecify(nemo_t * nemo, int type,const char * pattern,const size_t patternlen,char ** errptr){
	 	nemo_SaveError(errptr,nemo->rep->CheckMetaSpecify(static_cast<DBType>(type),std::string(pattern,patternlen)));
	}
	void nemo_ChecknRecover(nemo_t * nemo, int type,const char * key,const size_t keylen,char ** errptr){
	 	nemo_SaveError(errptr,nemo->rep->ChecknRecover(static_cast<DBType>(type),std::string(key,keylen)));
	}	 
	void nemo_HChecknRecover(nemo_t * nemo,const char * key,const size_t keylen,char ** errptr){
	 	nemo_SaveError(errptr,nemo->rep->HChecknRecover(std::string(key,keylen)));
	}
	void nemo_LChecknRecover(nemo_t * nemo,const char * key,const size_t keylen,char ** errptr){
	 	nemo_SaveError(errptr,nemo->rep->LChecknRecover(std::string(key,keylen)));
	}
	void nemo_SChecknRecover(nemo_t * nemo,const char * key,const size_t keylen,char ** errptr){
	 	nemo_SaveError(errptr,nemo->rep->SChecknRecover(std::string(key,keylen)));
	}
	void nemo_ZChecknRecover(nemo_t * nemo,const char * key,const size_t keylen,char ** errptr){
	 	nemo_SaveError(errptr,nemo->rep->ZChecknRecover(std::string(key,keylen)));
	}

	nemo_DBNemo_t * nemo_GetMetaHandle(nemo_t * nemo){
		nemo_DBNemo_t * db = new nemo_DBNemo_t;
		db->rep = nemo->rep->GetMetaHandle();
		return db;
	}

	nemo_DBNemo_t * nemo_GetRaftHandle(nemo_t * nemo){
		nemo_DBNemo_t * db = new nemo_DBNemo_t;
		db->rep = nemo->rep->GetRaftHandle();
		return db;
	}

	nemo_DBNemo_t * nemo_GetKvHandle(nemo_t * nemo){
		nemo_DBNemo_t * db = new nemo_DBNemo_t;
		db->rep = nemo->rep->GetKvHandle();
		return db;
	}

	nemo_WriteBatch_t * createWriteBatch()
	{
		nemo_WriteBatch_t * nwb = new nemo_WriteBatch_t();
		nwb->rep = rocksdb::WriteBatch();
		return nwb;
	}

	void rocksdb_WriteBatch_Put(nemo_WriteBatch_t * nwb, const char * key, const size_t keylen, 
												 const char * value ,const size_t vallen )
	{
		rocksdb::Slice keystr(key,keylen);
		rocksdb::Slice valstr(value,vallen);
		nwb->rep.Put(keystr,valstr);
	}
	void rocksdb_WriteBatch_Del(nemo_WriteBatch_t * nwb,  const char * key, const size_t keylen)
	{
		rocksdb::Slice keystr(key,keylen);
		nwb->rep.Delete(keystr);
	}
	void rocksdb_BatchWrite(nemo_t * nemo,nemo_DBNemo_t * db,nemo_WriteBatch_t * nwb, bool sync ,char ** errptr)
	{
	 	nemo_SaveError(errptr,nemo->rep->BatchWrite(db->rep,&(nwb->rep),sync));
		delete nwb;
	}

	void nemo_PutWithHandle(nemo_t * nemo,nemo_DBNemo_t * db, 
								const char * key, const size_t keylen, 
								const char * value ,const size_t vallen,
								bool sync,
								char ** errptr)
	{
		rocksdb::Slice keystr(key,keylen);
		rocksdb::Slice valstr(value,vallen);
		nemo_SaveError(errptr,nemo->rep->PutWithHandle(db->rep,keystr,valstr,sync));
	}

	void * nemo_GetWithHandle(nemo_t * nemo,nemo_DBNemo_t * db, 
								const char * key, const size_t keylen, 
								const char ** value ,size_t* vallen,
								char ** errptr)
	{
		rocksdb::Slice keystr(key,keylen);
		std::string * valstr = new std::string();
		Status s = nemo->rep->GetWithHandle(db->rep,keystr,valstr);

		if(s.ok())
		{
			*errptr = nullptr;
			*value = valstr->data();
			*vallen  = valstr->size();
		}
		else if (s.IsNotFound())
		{
			*errptr = nullptr;
			*value = nullptr;
			*vallen  = 0;
			valstr = nullptr;
		}
		else
		{
			*errptr = strdup(s.ToString().c_str());
			valstr = nullptr;
		}
		return (void *)valstr;
	}

	void nemo_DeleteWithHandle(nemo_t * nemo,nemo_DBNemo_t * db, 
								const char * key, const size_t keylen, 
								bool sync,
								char ** errptr)
	{
		rocksdb::Slice keystr(key,keylen);
		nemo_SaveError(errptr,nemo->rep->DeleteWithHandle(db->rep,keystr,sync));	
	}

	nemo_VolumeIterator_t * createVolumeIterator(nemo_t * nemo,
								const char * start, const size_t startlen, 
								const char * end ,const size_t endlen,
								bool use_snapshot)
	{
		nemo_VolumeIterator_t * it = new nemo_VolumeIterator_t;
		std::string startstr(start,startlen);
		std::string endstr(end,endlen);	
		it->rep = new nemo::VolumeIterator(nemo->rep,startstr,endstr,1LL << 60,use_snapshot);
		return it;
	}

	void VolNext(nemo_VolumeIterator_t * it)
	{
		it->rep->Next();
	}
	bool VolValid(nemo_VolumeIterator_t * it){
		return it->rep->Valid();	
	}
	const char* Volkey(nemo_VolumeIterator_t * it, size_t* keylen)
	{
		*keylen = it->rep->key().size();
		return it->rep->key().data();		
	}
	void Volvalue(nemo_VolumeIterator_t * it,int64_t * value)
	{
		*value = it->rep->value();
	}
	bool VoltargetScan(nemo_VolumeIterator_t * it,int64_t target)
	{
		return it->rep->targetScan(target);
	}
	int64_t VoltotalVolume(nemo_VolumeIterator_t * it){
		return it->rep->totalVolume();
	}	
	void VoltargetKey(nemo_VolumeIterator_t * it,char ** key ,size_t* keylen){
		*key = CopyString(it->rep->targetKey());
		*keylen = it->rep->targetKey().size();
	}
	void VolIteratorFree(nemo_VolumeIterator_t * it)
	{
		delete it->rep;
		delete it;
	}

	void nemo_RangeDel(nemo_t * nemo,const char * start, const size_t startlen, 
								const char * end ,const size_t endlen,
								char ** errptr)
	{
		std::string startstr(start,startlen);
		std::string endstr(end,endlen);	
		nemo_SaveError(errptr,nemo->rep->RangeDel(startstr,endstr));	
	}

	void nemo_RangeDelWithHandle(nemo_t * nemo, nemo_DBNemo_t * db,
								const char * start, const size_t startlen, 
								const char * end ,const size_t endlen,
								char ** errptr)
	{
		std::string startstr(start,startlen);
		std::string endstr(end,endlen);	
		nemo_SaveError(errptr,nemo->rep->RangeDelWithHandle(db->rep,startstr,endstr));	
	}

	void nemo_RawScanSaveAll(nemo_t * nemo, const char * path, const char * start, size_t startlen,
												const char * end, size_t endlen,
												bool use_snapshot,char ** errptr)
	{
		nemo_SaveError(errptr,nemo->rep->RawScanSaveAll(std::string(path),std::string(start,startlen),
																		  std::string(end,endlen),use_snapshot));
	}

	void nemo_IngestFile(nemo_t * nemo, const char * path, char ** errptr)
	{
		nemo_SaveError(errptr,nemo->rep->IngestFile(std::string(path)));
	}

} // end of extern "C"

