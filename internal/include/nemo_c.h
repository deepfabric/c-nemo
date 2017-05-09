#ifndef __INCLUDE_NEMO_C_H_
#define __INCLUDE_NEMO_C_H_

#pragma once

#ifdef __cplusplus
extern "C" {
#endif

#include <stdarg.h>
#include <stddef.h>
#include <stdint.h>
#include <stdbool.h>

typedef	struct nemo_t nemo_t;
typedef	struct nemo_options_t nemo_options_t;
typedef	struct nemo_Iterator_t nemo_Iterator_t;
typedef	struct nemo_KIterator_t nemo_KIterator_t;
typedef	struct nemo_HIterator_t nemo_HIterator_t;
typedef	struct nemo_ZIterator_t nemo_ZIterator_t;
typedef	struct nemo_SIterator_t nemo_SIterator_t;
typedef struct nemo_Snaptshot_t nemo_Snaptshot_t;
typedef struct nemo_DBNemo_t nemo_DBNemo_t;

typedef struct nemo_VolumeIterator_t nemo_VolumeIterator_t;
typedef struct nemo_DBWithTTL_t nemo_DBWithTTL_t;
typedef struct nemo_WriteBatch_t nemo_WriteBatch_t;


//c style string structure as return value type of C API function
//member str point to a c++ string object which is dynamic allocate,
//it must be explicit released by member str point
// cstyle string structure, as input agument type f C API function 
typedef struct{
  const char *  data;
  size_t        len;
  void       *  str;
} nemoStr;

enum  {
  kNONE_DB = 0,
  kKV_DB,
  kHASH_DB,
  kLIST_DB,
  kZSET_DB,
  kSET_DB,
  kALL
};

enum  {
  BEFORE = 0,
  AFTER = 1
};

enum  {
  SUM = 0,
  MIN,
  MAX
};

void nemoStrFree(nemoStr * str);

extern nemo_t * nemo_Create(const char * db_path,const nemo_options_t * options);

extern nemo_options_t * nemo_CreateOption();

extern void nemo_Compact(nemo_t * nemo,int db_type,bool sync,char ** errptr);

extern void nemo_RunBGTask(nemo_t * nemo,char ** errptr);

extern char * nemo_GetCurrentTaskType(nemo_t * nemo);

extern void nemo_free(nemo_t * instance);

// =================String=====================
extern void nemo_Del(nemo_t * nemo,const char * key, const size_t keylen ,int64_t *count,char ** errptr);

extern void nemo_MDel(nemo_t * nemo,const int num, const char ** keys_list,const size_t * kyes_list_size, int64_t *count,char ** errptr);

extern 	void nemo_Expire(nemo_t * nemo,const char * key, const size_t keylen, int32_t  seconds, int64_t * res,char ** errptr);

extern void nemo_TTL(nemo_t * nemo,const char * key,const size_t keylen, int64_t *res,char ** errptr);

extern void nemo_Persist(nemo_t * nemo,const char * key, const size_t keylen, int64_t *res,char ** errptr);

extern void nemo_Expireat(nemo_t * nemo,const char * key, const size_t keylen, const int32_t  timestamp, int64_t  *res,char ** errptr);

extern void nemo_Type(nemo_t * nemo,const char * key,const size_t keylen,  char ** type,char ** errptr);

extern void nemo_Exists(nemo_t * nemo,const int num, const char ** key_list,const size_t * key_list_size,int64_t * res,char ** errptr);

// =================KV=====================
extern void nemo_Set(nemo_t * nemo,const char * key, const size_t keylen, const char * val, const size_t vallen, int32_t ttl, char ** errptr);

extern void nemo_Get(nemo_t * nemo,const char * key, const size_t keylen, const char ** val,size_t * vallen, char ** errptr);
extern void nemo_Get0(nemo_t * nemo,const char * key, const size_t keylen, nemoStr * value, char ** errptr);

extern void nemo_MSet(nemo_t * nemo, int const num,const char ** key, size_t * keylen, \
                                                   const char ** val, size_t * vallen, char ** errptr);

extern void nemo_MGet(nemo_t * nemo,  const int num, const char ** key, size_t * keylen, \
	 		             		                                     char ** val, size_t * vallen, char ** errs);

extern void nemo_Scan(nemo_t * nemo, int64_t cursor, const char * pattern, const size_t patternlen, int64_t count, \
				           int * key_num, char *** key_list,size_t ** key_list_strlen, int64_t * cursor_ret, char ** errptr);

extern 	void nemo_Keys(nemo_t * nemo,  char * pattern,const size_t patternlen, int * key_num, \
					                             char *** key_list, size_t ** key_list_strlen , char ** errptr);

extern void nemo_KMDel(nemo_t * nemo,const int num,const char ** key_list,size_t * key_list_size,int64_t *count, char ** errptr);

extern void nemo_Incrby(nemo_t * nemo,const char * key,const size_t keylen,const int64_t by, char ** new_val,size_t * new_val_len,char ** errptr);

extern void nemo_Decrby(nemo_t * nemo,const char * key,const size_t keylen,const int64_t by, char ** new_val,size_t * new_val_len,char ** errptr);

extern void nemo_Incrbyfloat(nemo_t * nemo,const char * key,const size_t keylen,const double by, char ** new_val,size_t * new_val_len,char ** errptr);

extern void nemo_GetSet(nemo_t * nemo,const char * key,const size_t keylen,const char * value, const size_t vallen,char ** old_val,size_t * old_val_len,char ** errptr);

extern void nemo_Append(nemo_t * nemo,const char * key,const size_t keylen,const char * value,const size_t vallen, int64_t * new_len,char ** errptr);

extern void nemo_Setnx(nemo_t * nemo,const char * key,const size_t keylen,const char * value, const size_t vallen,int64_t * ret,const int32_t ttl,char ** errptr);

extern void nemo_Setxx(nemo_t * nemo,const char * key,const size_t keylen,const char * value,const size_t vallen, int64_t * ret,const int32_t ttl,char ** errptr);

extern void nemo_MSetnx(nemo_t * nemo,const int num, const char ** key,const size_t * keylen, \
                          const char ** val,const size_t * vallen,  \
                          int64_t * ret,char ** errptr);

extern void nemo_Getrange(nemo_t * nemo,const char * key,const size_t keylen,const int64_t start,const int64_t end, char ** substr,size_t * substr_len, char ** errptr);

extern void nemo_Setrange(nemo_t * nemo,const char * key,const size_t keylen,const int64_t offset,const char * value,const size_t vallen,int64_t * len ,char ** errptr);

extern void nemo_Strlen(nemo_t * nemo,const char * key,const size_t keylen,int64_t * len, char ** errptr);


// ==============BITMAP=====================
extern void nemo_BitSet(nemo_t * nemo,const char * key,const size_t keylen,const int64_t offset,const int64_t on, int64_t * res, char ** errptr);

extern void nemo_BitGet(nemo_t * nemo,const char * key,const size_t keylen,const int64_t offset,int64_t * res, char ** errptr);

extern void nemo_BitCount(nemo_t * nemo,const char * key,const size_t keylen,int64_t * res, char ** errptr);

extern void nemo_BitCountRange(nemo_t * nemo,const char * key,const size_t keylen,int64_t start,int64_t end,int64_t * res, char ** errptr);

extern void nemo_BitPos(nemo_t * nemo,const char * key,const size_t keylen,const int64_t bit,int64_t * res, char ** errptr);

extern void nemo_BitPosWithStart(nemo_t * nemo,const char * key,const size_t keylen,const int64_t bit,const int64_t start,int64_t * res, char **      errptr);

extern void nemo_BitPosWithStartEnd(nemo_t * nemo,const char * key,const size_t keylen,const int64_t bit, \
                                    const int64_t start,const int64_t end,int64_t * res, char ** errptr);

extern void nemo_BitOp(nemo_t * nemo,int optype,const char * dest_key,const size_t destlen, \
                        const int num, char ** src_key_list, size_t * src_key_len, int64_t * result_length, char ** errptr);

// ==============HASH=====================
extern void nemo_HSet(nemo_t * nemo,const char * key,const size_t keylen,const char * field,const size_t fieldlen,const char * value,const size_t vallen, char ** errptr);

extern void nemo_HGet(nemo_t * nemo,const char * key,const size_t keylen,const char * field,const size_t fieldlen,char ** value, size_t * value_len , char ** errptr);

extern void nemo_HDel(nemo_t * nemo,const char * key,const size_t keylen,const char * field, const size_t fieldlen,char ** errptr);

extern bool nemo_HExists(nemo_t * nemo,const char * key,const size_t keylen,const char * field,const size_t fieldlen);

extern void nemo_HKeys(nemo_t * nemo,const char * key,const size_t keylen,int * count, char *** field_list, size_t ** field_list_strlen, char ** errptr);

extern void nemo_HGetall(nemo_t * nemo,const char * key, const size_t keylen,int * count, char *** field_list, size_t ** field_list_strlen, \
                                                                          char *** value_list, size_t ** value_list_strlen, char ** errptr);
extern int64_t nemo_HLen(nemo_t * nemo,const char * key,const size_t keylen);

extern void nemo_HMSet(nemo_t * nemo,const char * key, const size_t keylen,const int num, const char ** field_list,const size_t * field_list_len, \
                                   const char ** value_list,const size_t * value_list_len, char ** errptr);

extern void nemo_HMGet(nemo_t * nemo,const char * key, const size_t keylen,const int num, const char ** field_list,const size_t * field_list_len, \
                                   char ** value_list,size_t * value_list_strlen, char ** errs,char ** errptr); 

extern void nemo_HSetnx(nemo_t * nemo,const char * key,const size_t keylen,const char * field,const size_t fieldlen,const char * value, const size_t vallen,char ** errptr);

extern int64_t nemo_HStrlen(nemo_t * nemo,const char * key,const size_t keylen,const char * field,const size_t fieldlen);

extern void nemo_HVals(nemo_t * nemo,const char * key,const size_t keylen,char *** val_list, size_t ** val_list_strlen, int * count, char ** errptr);

extern void nemo_HIncrby(nemo_t * nemo,const char * key,const size_t keylen,const char * field,const size_t fieldlen,const int64_t by, char ** new_val, size_t * new_val_len, char ** errptr);

extern void nemo_HIncrbyfloat(nemo_t * nemo,const char * key,const size_t keylen,const char * field,const size_t fieldlen,const double by, char ** new_val, size_t * new_val_len, char ** errptr);

  // ==============List=====================
extern void nemo_LIndex(nemo_t * nemo,const char * key, const size_t keylen, const int64_t index, char ** val, size_t * val_len, char ** errptr);

extern void nemo_LLen(nemo_t * nemo,const char * key,const size_t keylen, int64_t * llen,char ** errptr);

extern void nemo_LPush(nemo_t * nemo,const char * key,const size_t keylen,const char * value,const size_t vallen, int64_t * llen,char ** errptr);

extern void nemo_LPop(nemo_t * nemo,const char * key,const size_t keylen,char ** value, size_t * value_len, char ** errptr);

extern void nemo_LPushx(nemo_t * nemo,const char * key,const size_t keylen,const char * value, const size_t vallen,int64_t * llen,char ** errptr);

extern void nemo_LRange(nemo_t * nemo,const char * key,const size_t keylen,const int64_t begin,const int64_t end, \
					size_t * num, int64_t ** index_list,char *** val_list, size_t ** val_list_strlen, char ** errptr);

extern void nemo_LSet(nemo_t * nemo,const char * key,const size_t keylen, const int64_t index, char * val,const size_t vallen,char ** errptr);

extern void nemo_LTrim(nemo_t * nemo,const char * key,const size_t keylen, const int64_t begin, const int64_t end,char ** errptr);

extern void nemo_RPush(nemo_t * nemo,const char * key,const size_t keylen,const char * value,const size_t vallen, int64_t * llen,char ** errptr);

extern 	void nemo_RPop(nemo_t * nemo,const char * key,const size_t keylen,char ** value, size_t * value_len, char ** errptr);

extern 	void nemo_RPushx(nemo_t * nemo,const char * key,const size_t keylen,const char * value,const size_t vallen, int64_t * llen,char ** errptr);

extern 	void nemo_RPopLPush(nemo_t * nemo,const char * src,const size_t srclen,char * dest,const size_t destlen,char **val, size_t * val_len, char ** errptr);

extern	void nemo_LInsert(nemo_t * nemo,const char * key,const size_t keylen, int pos,\
								    const char * pivot,const size_t pivotlen,\
									const char * val,const size_t vallen,int64_t *llen,char ** errptr);

extern	void nemo_LRem(nemo_t * nemo,const char * key,const size_t keylen, const int64_t count, const char * val, const size_t vallen,int64_t * rem_count,char ** errptr);

  // ==============Set=====================

extern 	void nemo_SAdd(nemo_t * nemo,const char * key,const size_t keylen, const char * member,const size_t memberlen,int64_t *res,char ** errptr);

extern 	void nemo_SRem(nemo_t * nemo,const char * key,const size_t keylen, const char * member,const size_t memberlen,int64_t *res,char ** errptr);

extern 	int64_t nemo_SCard(nemo_t * nemo,const char * key,const size_t keylen);

extern 	void nemo_SMembers(nemo_t * nemo,const char * key,const size_t keylen,char *** member_list, size_t ** member_list_strlen,int * count, char ** errptr);

extern	void nemo_SUnionStore(nemo_t * nemo,const char * destination, const size_t destlen,\
							const int num, const char ** key_list,const size_t * key_list_len,	\
																  int64_t * res, char ** errptr);

extern 	void nemo_SUnion(nemo_t * nemo, const int num, const char ** key_list,const size_t * key_list_len,		\
				         int * count,char *** val_list, size_t ** val_list_strlen, char ** errptr);                                  

extern 	void nemo_SInterStore(nemo_t * nemo,const char * destination, const size_t destlen , \
						const int num, const char ** key_list,const size_t * key_list_len,	\
																  int64_t * res, char ** errptr);

extern 	void nemo_SInter(nemo_t * nemo, const int num, const char ** key_list,const size_t * key_list_len,		\
					int * count,char *** val_list, size_t ** val_list_strlen, char ** errptr);

extern	void nemo_SDiffStore(nemo_t * nemo,const char * destination,const size_t destlen, const int num, const char ** key_list,const size_t * key_list_len,		\
																  int64_t * res, char ** errptr);

extern	void nemo_SDiff(nemo_t * nemo, const int num, const char ** key_list,const size_t * key_list_len,		\
									int * count,char *** val_list, size_t ** val_list_strlen, char ** errptr);
		
extern 	bool nemo_SIsMember(nemo_t * nemo,const char * key,const size_t keylen, const char * member,const size_t memlen);

extern 	void nemo_SPop(nemo_t * nemo,const char * key,const size_t keylen,  char ** member, size_t * len,  char ** errptr);

extern 	void nemo_SRandomMember(nemo_t * nemo,  const char * key,const size_t keylen,		\
						int * res_count,char ** member_list, size_t * member_list_strlen, const int count, char ** errptr);

extern  void nemo_SMove(nemo_t * nemo,const char * source,const size_t slen,const char * dest,const size_t destlen,\
						const char * member,const size_t memlen,int64_t * res,char ** errptr);
// ==============ZSet=====================

extern void nemo_ZAdd(nemo_t * nemo,const char * key, const size_t keylen, const double score, const char * member,const size_t memlen,int64_t *res,char ** errptr);

extern int64_t nemo_ZCard(nemo_t * nemo,const char * key,const size_t keylen);

extern int64_t nemo_ZCount(nemo_t * nemo,const char * key,const size_t keylen,const double begin,const double end ,bool is_lo,bool is_ro);

extern void nemo_ZIncrby(nemo_t * nemo,const char * key,const size_t keylen, \
				const char * member,const size_t memlen,const double by, char ** new_val,size_t * new_val_len,char ** errptr);
        
extern void nemo_ZRange(nemo_t * nemo,const char * key,const size_t keylen,const int64_t start,const int64_t stop,\
    						size_t * num,double ** score_list,char *** member_list,size_t ** member_list_strlen,char ** errptr);

extern void nemo_ZUnionStore(nemo_t * nemo,const char * destination, const size_t destlen,const int numkeys, 	\
						  const int list_len,const char ** key_list,const size_t * key_list_len,const double * weight_list,	\
						  int agg,int64_t * res, char ** errptr);

extern void nemo_ZInterStore(nemo_t * nemo,const char * destination,const size_t destlen, const int numkeys, 	\
						  const int list_len,const char ** key_list,const size_t * key_list_len,const double * weight_list,	\
						  int agg,int64_t * res, char ** errptr);

extern void nemo_ZRangebyScore(nemo_t * nemo,const char * key,const size_t keylen,const double mn,const double mx,\
    						int * count,double ** score_list,char *** member_list, size_t ** member_list_strlen,\
							bool is_lo, bool is_ro, char ** errptr);
                
extern void nemo_ZRem(nemo_t * nemo,const char * key,const size_t keylen, const char * member,const size_t memlen,int64_t *res,char ** errptr);

extern void nemo_ZRank(nemo_t * nemo,const char * key,const size_t keylen, const char * member,const size_t memlen,int64_t *rank,char ** errptr);

extern void nemo_ZRevrank(nemo_t * nemo,const char * key, const size_t keylen, const char * member,const size_t memlen,int64_t *rank,char ** errptr);

extern void nemo_ZScore(nemo_t * nemo,const char * key, const size_t keylen, const char * member,const size_t memlen, double * score,char ** errptr);

extern void nemo_ZRangebylex(nemo_t * nemo,const char * key,const size_t keylen,const char * min,const size_t minlen,const char * max, const size_t maxlen, \
				int * num,char *** member_list, size_t ** member_list_strlen,char ** errptr);

extern void nemo_ZLexcount(nemo_t * nemo,const char * key,const size_t keylen,const char * min,const size_t minlen,const char * max,const size_t maxlen,int64_t * count,char ** errptr);

extern void nemo_ZRemrangebylex(nemo_t * nemo,const char * key,const size_t keylen,const char * min,const size_t minlen,const char * max,const size_t maxlen,bool is_lo,bool is_ro,int64_t * count,char ** errptr);

extern void nemo_ZRemrangebyrank(nemo_t * nemo,const char * key,const size_t keylen,const int64_t start,const int64_t stop,int64_t * count,char ** errptr);

extern void nemo_ZRemrangebyscore(nemo_t * nemo,const char * key,const size_t keylen,const double start,const double stop,int64_t * count,bool is_lo,bool is_ro,char ** errptr);

// ==============HyperLogLog=====================
extern void nemo_PfAdd(nemo_t * nemo,const char * key,const size_t keylen,const int num,const char ** value_list,size_t * value_list_len,bool * update,char ** errptr);
extern void nemo_PfCount(nemo_t * nemo,const int num,const char ** key_list,const size_t * key_list_len,int *res,char ** errptr);

extern void nemo_PfMerge(nemo_t * nemo,const int num,const char ** key_list,const size_t * key_list_len,char ** errptr);

// ==============Server=====================
extern void nemo_BGSaveGetSnapshot(nemo_t * nemo,int * count,nemo_Snaptshot_t ** snapshot_list,char ** errptr);

extern void nemo_BGSaveSpecify(nemo_t * nemo,const char * key_type,nemo_Snaptshot_t * snapshot,char ** errptr);

extern void nemo_BGSaveGetSpecifySnapshot(nemo_t * nemo,const char * key_type,nemo_Snaptshot_t ** snapshot,char ** errptr);

extern void nemo_BGSave(nemo_t * nemo,int len,nemo_Snaptshot_t * snapshot_list,const char * db_path,char ** errptr);

extern void nemo_BGSaveOff(nemo_t * nemo,char ** errptr);

extern void nemo_GetKeyNum(nemo_t * nemo,int * count,long long unsigned int ** num,char ** errptr);

extern void nemo_GetSpecifyKeyNum(nemo_t * nemo,const char * type,long long unsigned int * num,char ** errptr);

extern void nemo_GetUsage(nemo_t * nemo,const char * type,long long unsigned int * res,char ** errptr);

extern nemo_DBNemo_t * nemo_GetDBByType(nemo_t * nemo,const char * type);

extern 	void nemo_CheckMetaSpecify(nemo_t * nemo, int type,const char * pattern,const size_t patternlen,char ** errptr);

extern void nemo_ChecknRecover(nemo_t * nemo, int type,const char * key,const size_t keylen,char ** errptr);

extern void nemo_HChecknRecover(nemo_t * nemo,const char * key,const size_t keylen,char ** errptr);

extern void nemo_LChecknRecover(nemo_t * nemo,const char * key,const size_t keylen,char ** errptr);

extern void nemo_SChecknRecover(nemo_t * nemo,const char * key,const size_t keylen,char ** errptr);

extern void nemo_ZChecknRecover(nemo_t * nemo,const char * key,const size_t keylen,char ** errptr);

extern nemo_DBWithTTL_t * nemo_GetMetaHandle(nemo_t * nemo);
extern nemo_DBWithTTL_t * nemo_GetRaftHandle(nemo_t * nemo);
extern nemo_WriteBatch_t * createWriteBatch();
extern void rocksdb_WriteBatch_Put(nemo_WriteBatch_t * nwb, const char * key, const size_t keylen, 
												 const char * value ,const size_t vallen );
extern void rocksdb_WriteBatch_Del(nemo_WriteBatch_t * nwb,  const char * key, const size_t keylen, 
												  const char * value ,const size_t vallen );
extern void rocksdb_BatchWrite(nemo_t * nemo,nemo_DBWithTTL_t * db,nemo_WriteBatch_t * nwb,char ** errptr);                                                  
extern void nemo_PutWithHandle(nemo_t * nemo,nemo_DBWithTTL_t * db, 
								const char * key, const size_t keylen, 
								const char * value ,const size_t vallen,
								char ** errptr);
extern void nemo_GetWithHandle(nemo_t * nemo,nemo_DBWithTTL_t * db, 
								const char * key, const size_t keylen, 
								char ** value ,size_t* vallen,
								char ** errptr);                

extern 	nemo_KIterator_t *	nemo_KScanWithHandle(nemo_t * nemo,nemo_DBWithTTL_t * db,
								const char * start, const size_t startlen, 
								const char * end ,const size_t endlen,
								uint64_t limit, bool use_snapshot);
extern void KNext(nemo_KIterator_t * it);
extern bool KValid(nemo_KIterator_t * it);
extern void Kkey(nemo_KIterator_t * it,char ** key ,size_t* keylen);
extern void Kvalue(nemo_KIterator_t * it,char ** value ,size_t* valuelen);
extern void KIteratorFree(nemo_KIterator_t * it);
extern nemo_VolumeIterator_t * createVolumeIterator(nemo_t * nemo,
								const char * start, const size_t startlen, 
								const char * end ,const size_t endlen,
								uint64_t limit, bool use_snapshot);
extern void VolNext(nemo_VolumeIterator_t * it);
extern bool VolValid(nemo_VolumeIterator_t * it);
extern void Volkey(nemo_VolumeIterator_t * it,char ** key ,size_t* keylen);
extern void Volvalue(nemo_VolumeIterator_t * it,int64_t * value);
extern void VolIteratorFree(nemo_VolumeIterator_t * it);
extern 	void nemo_RangeDel(nemo_t * nemo,const char * start, const size_t startlen, 
								const char * end ,const size_t endlen,
								uint64_t limit);
extern 	void nemo_RangeDelWithHandle(nemo_t * nemo, nemo_DBWithTTL_t * db,
								const char * start, const size_t startlen, 
								const char * end ,const size_t endlen,
								uint64_t limit);
                                






#ifdef __cplusplus
}
#endif

#endif // end of __NEMO_C_H_
