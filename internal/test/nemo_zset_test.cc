#include <string>
#include <vector>
#include <sys/time.h>
#include <cstdlib>
#include <cstdlib>
#include <string>
#include <cmath>

#include "gtest/gtest.h"
#include "xdebug.h"
#include "nemo.h"

//#include "stdint.h"
#include "nemo_zset_test.h"
using namespace std;


TEST_F(NemoZSetTest, TestZAdd) {
	log_message("============================ZSETTEST START===========================");
	log_message("============================ZSETTEST START===========================");
	log_message("========TestZAdd========");

	string key, member;
	int64_t res;
	double score, scoreGet;

	s_.OK();//ԭ����key��member��Ӧ��ֵ������
	key = GetRandomKey_();
	member = GetRandomVal_();
	score = GetRandomFloat_();
	s_ = n_->ZAdd(key, score, member, &res);
	CHECK_STATUS(OK);
	EXPECT_EQ(1, res);
	n_->ZScore(key, member, &scoreGet);
	EXPECT_TRUE(isDoubleEqual(score, scoreGet));
	if (s_.ok() && res == 1 && isDoubleEqual(score,scoreGet)) {
		log_success("ԭ����key��member��Ӧ��ֵ������");
	} else {
		log_fail("ԭ����key��member��Ӧ��ֵ������");
	}

	s_.OK();//ԭ����key��member��Ӧ��ֵ����
	score = GetRandomFloat_();
	s_ = n_->ZAdd(key, score, member, &res);
	CHECK_STATUS(OK);
	EXPECT_EQ(0, res);
	n_->ZScore(key, member, &scoreGet);
	EXPECT_TRUE(isDoubleEqual(score, scoreGet));
	if (s_.ok() && res == 0 && isDoubleEqual(score, scoreGet)) {
		log_success("ԭ����key��member��Ӧ��ֵ����");
	} else {
		log_fail("ԭ����key��member��Ӧ��ֵ����");	
	}

	s_.OK();//ԭ����key��member��Ӧ��ֵ���ڣ���score��ԭ����ֵ��eps��10-5����Χ��
	double scorePre = score;
	score = score+0.0000005;
	s_ = n_->ZAdd(key, score, member, &res);
	CHECK_STATUS(OK);
	EXPECT_EQ(0, res);
	n_->ZScore(key, member, &scoreGet);
	EXPECT_TRUE(isDoubleEqual(scorePre, scoreGet));
	if (s_.ok() && res == 0 && isDoubleEqual(scorePre, scoreGet)) {
		log_success("ԭ����key��member��Ӧ��ֵ���ڣ���score��ԭ����ֵ��eps��10-5����Χ��");
	} else {
		log_fail("ԭ����key��member��Ӧ��ֵ���ڣ���score��ԭ����ֵ��eps��10-5����Χ��");
	}

	s_.OK();//key��Ϊ�գ�memberΪ��
	key = GetRandomKey_();
	member = "";
	score = GetRandomFloat_();
	s_ = n_->ZAdd(key, score, member, &res);
	CHECK_STATUS(OK);
	EXPECT_EQ(1, res);
	n_->ZScore(key, member, &scoreGet);
	EXPECT_TRUE(isDoubleEqual(score, scoreGet));
	if (s_.ok() && 1 == res && isDoubleEqual(score, scoreGet)) {
		log_success("key��Ϊ�գ�memberΪ��");
	} else {
		log_fail("key��Ϊ�գ�memberΪ��");
	}

	s_.OK();//keyΪ�գ�member��Ϊ��
	key = "";
	member = GetRandomVal_();
	score = GetRandomFloat_();
	s_ = n_->ZAdd(key, score, member, &res);
	CHECK_STATUS(OK);
	EXPECT_EQ(1, res);
	n_->ZScore(key, member, &scoreGet);
	EXPECT_TRUE(isDoubleEqual(score, scoreGet));
	if (s_.ok() && res == 1 && isDoubleEqual(score, scoreGet)) {
		log_success("keyΪ�գ�member��Ϊ��");
	} else {
		log_fail("keyΪ�գ�member��Ϊ��");
	}
}

TEST_F(NemoZSetTest, TestZCard) {
	log_message("\n========TestZCard========");

	string key, member;
	int64_t resTemp, card;

	s_.OK();//ԭ����key������
	key = GetRandomKey_();
	n_->ZRemrangebyscore(key, ZSET_SCORE_MIN, ZSET_SCORE_MAX, &resTemp);
	card = n_->ZCard(key);
	EXPECT_EQ(0, card);
	if (card == 0) {
		log_success("ԭ����key������");
	} else {
		log_fail("ԭ����key������");
	}

	s_.OK();//ԭ����key����
	key = "ZCardTest";
	int64_t num = 10;
	write_zset_random_score(key, num);
	card = n_->ZCard(key);
	EXPECT_EQ(num, card);
	if (card == num) {
		log_success("ԭ����key����");
	} else {
		log_fail("ԭ����key����");
	}
}

TEST_F(NemoZSetTest, TestZCount) {
	log_message("\n========TestZCount========");

	string key, member;
	int64_t count, num, res;
	double begin, end, score;

	s_.OK();//key������/key���ڣ���û��zset�ṹ
	key = GetRandomKey_();
	count = n_->ZCount(key, ZSET_SCORE_MIN, ZSET_SCORE_MAX);
	EXPECT_EQ(0, count);
	if (count == 0) {
		log_success("key������/key���ڣ���û��zset�ṹ");
	} else {
		log_fail("key������/key���ڣ���û��zset�ṹ");
	}
	
	s_.OK();//key���ڣ��Ҵ����zset�ṹ��begin<end
	key = "ZCount_Test";
	num = 20;
	write_zset_up_score(key, 20);
	begin = GetRandomFloat_(0, num-2);
	end = GetRandomFloat_(ceil(begin), num-1);
	count = n_->ZCount(key, begin, end);
	EXPECT_EQ(floor(end)-ceil(begin)+1, count);
	if (floor(end)-ceil(begin)+1 == count) {
		log_success("floor(end)-ceil(begin)+1, count");
	} else {
		log_fail("floor(end)-ceil(begin)+1, count");
	}

	s_.OK();//key���ڣ��Ҵ����zset�ṹ��begin>end
	begin = GetRandomFloat_(1, num-2);
	end = GetRandomFloat_(0, begin-1);
	count = n_->ZCount(key, begin, end);
	EXPECT_EQ(0, count);
	if (count == 0) {
		log_success("key���ڣ��Ҵ����zset�ṹ��begin>end");
	} else {
		log_fail("key���ڣ��Ҵ����zset�ṹ��begin>end");
	}

	s_.OK();//key���ڣ��Ҵ����zset�ṹ��begin<end;����is_lo=true�Ƿ�������
	key = GetRandomKey_();
	member = GetRandomVal_();
	score = 3.000012;
	n_->ZAdd(key, score, member, &res);
	member = GetRandomVal_();
	score = 3.000032;
	n_->ZAdd(key, score, member, &res);
	begin = 3.00001;
	end = ZSET_SCORE_MAX;
	count = n_->ZCount(key, begin, end, true);
	EXPECT_EQ(1, count);
	if (count == 1) {
		log_success("key���ڣ��Ҵ����zset�ṹ��begin<end;����is_lo=true�Ƿ�������");
	} else {
		log_fail("key���ڣ��Ҵ����zset�ṹ��begin<end;����is_lo=true�Ƿ�������");
	}

	s_.OK();//key���ڣ��Ҵ����zset�ṹ��begin<end;����is_ro=true�Ƿ�������
	begin = ZSET_SCORE_MIN;
	end = 3.00003;
	count = n_->ZCount(key, begin, end, false, true);
	EXPECT_EQ(1, count);
	if (1 == count) {
		log_success("key���ڣ��Ҵ����zset�ṹ��begin<end;����is_ro=true�Ƿ�������");
	} else {
		log_fail("key���ڣ��Ҵ����zset�ṹ��begin<end;����is_ro=true�Ƿ�������");
	}
}

TEST_F(NemoZSetTest, TestZScan) {
	log_message("\n========TestZScan========");

	string key, member;
	int64_t num, limit, count;
	double start, end;
	nemo::ZIterator* ziter;

	key = "ZScan_Test";
	num = 30;
	write_zset_up_score(key, num);
	
#define ZScanLoopProcess(countExpected, message)\
	ziter = n_->ZScan(key, start, end, limit);\
	count = 0;\
  for (; ziter->Valid(); ziter->Next())\
	{\
		count++;\
	}\
	EXPECT_EQ((countExpected), count);\
	if (count == (countExpected)) {\
		log_success(message);\
	} else {\
		log_fail(message);\
	}\
	do {} while(false)


	s_.OK();//start��end����scores��Χ�ڣ������������limit=-1
	start = GetRandomFloat_(0, num-2);
	end = GetRandomFloat_(ceil(start), num-1);
	limit = -1;
	ZScanLoopProcess(floor(end)-ceil(start)+1, "start��end����scores��Χ�ڣ������������limit=-1");

	s_.OK();//start��end����scores��Χ�ڣ������������len>limit>0
	start = ZSET_SCORE_MIN;
	end = ZSET_SCORE_MAX;
	limit = GetRandomUint_(1, num-1);
	ZScanLoopProcess(limit, "start��end����scores��Χ�ڣ������������len>limit>0");

	s_.OK();//start��end����scores��Χ�ڣ������������limit>len>0
	start = GetRandomFloat_(0, num-2);
	end = GetRandomFloat_(ceil(start), num-1);
	limit = num+10;
	ZScanLoopProcess(floor(end)-ceil(start)+1, "start��end����scores��Χ�ڣ������������limit>len>0");

	s_.OK();//start��end����scores��Χ�ڣ�start>end��,limit=-1
	start = GetRandomFloat_(1, num-1);
	end = GetRandomFloat_(0, floor(start));
	limit = -1;
	ZScanLoopProcess(0,  "start��end����scores��Χ�ڣ�start>end��,limit=-1");

	s_.OK();//start<scoreMin��end��scores��Χ��,limit=-1
	start = (-1)*GetRandomFloat_();
	end = GetRandomFloat_(0, num-1);
	limit = -1;
	ZScanLoopProcess(floor(end)+1, "start��end����scores��Χ�ڣ�start>end��,limit=-1");

	s_.OK();//start��scores��Χ�ڣ�endΪ>scoreMax,limit=-1
	start = GetRandomFloat_(0, num-1);
	end = num+10;
	limit = -1;
	ZScanLoopProcess(num-ceil(start), "start��scores��Χ�ڣ�endΪ>scoreMax,limit=-1");

	s_.OK();//start,end��>scoreMax,��start>end,limit=-1
	start = num+10;
	end = num+5;
	limit = -1;
	ZScanLoopProcess(0, "start,end��>scoreMax,��start>end,limit=-1");

	s_.OK();//start,end��>scoreMax��start<end,limit=-1
	start = num+10;
	end = num+20;
	limit = -1;
	ZScanLoopProcess(0, "start,end��>scoreMax��start<end,limit=-1");

	s_.OK();//start,end��<scoreMin��start<end,limit=-1
	start = -10;
	end = -5;
	limit = -1;
	ZScanLoopProcess(0, "start,end��<scoreMin��start<end,limit=-1");

	s_.OK();//start,end��<scoreMin��start>end,limit=-1
	start = -10;
	end = -20;
	limit = -1;
	ZScanLoopProcess(0, "start,end��<scoreMin��start<end,limit=-1");

	s_.OK();//start<scoreMin<scoreMax<end,limit=-1
	start = -10;
	end = num+10;
	limit = -1;
	ZScanLoopProcess(num, "start<scoreMin<scoreMax<end,limit=-1");

	s_.OK();//end<scoreMin<scoreMax<start,limit=-1
	end = -10;
	start = num+10;
	limit = -1;
	ZScanLoopProcess(0, "end<scoreMin<scoreMax<start,limit=-1");
}

TEST_F(NemoZSetTest, TestZIncrby) {
	log_message("\n=========TestZIncrby========");
	string key, member, newVal;
	double score, by, scoreGet;
	int64_t res;


	s_.OK();//key��member��Ӧ��score������
	key = GetRandomKey_();
	member = GetRandomVal_();
	by = GetRandomFloat_();
	s_ = n_->ZIncrby(key, member, by, newVal);
	CHECK_STATUS(OK);
	EXPECT_TRUE(isDoubleEqual(by, atof(newVal.c_str())));
	if (s_.ok() && isDoubleEqual(by, atof(newVal.c_str()))) {
		log_success("key��member��Ӧ��score������");
	} else {
		log_fail("key��member��Ӧ��score������");
	}

	s_.OK();//key��member��Ӧ��score����
	s_ = n_->ZIncrby(key, member, by, newVal);
	CHECK_STATUS(OK);
	EXPECT_TRUE(isDoubleEqual(2*by, atof(newVal.c_str())));
	if (s_.ok() && isDoubleEqual(2*by, atof(newVal.c_str()))) {
		log_success("key��member��Ӧ��score����");
	} else {
		log_fail("key��member��Ӧ��score����");
	}

	s_.OK();//key��member��Ӧ��score������,by�����ƣ�����10000000000000LL��
	key = GetRandomKey_();
	member = GetRandomVal_();
	by = ZSET_SCORE_MAX*1.5;
	newVal.clear();
	s_ = n_->ZIncrby(key, member, by, newVal);
	CHECK_STATUS(Corruption);
	EXPECT_TRUE(newVal.empty());
	if (s_.IsCorruption() && newVal.empty()) {
		log_success("key��member��Ӧ��score������,by�����ƣ�����10000000000000LL��");
	} else {
		log_fail("key��member��Ӧ��score������,by�����ƣ�����10000000000000LL��");
	}

	s_.OK();//key��member��Ӧ��score����,��by�����ƣ�����10000000000000LL��
	key = GetRandomKey_();
	member = GetRandomVal_();
	score = ZSET_SCORE_MIN+100;
	n_->ZAdd(key, score, member, &res);
	by = -1000.0f;
	newVal.clear();
	s_ = n_->ZIncrby(key, member, by, newVal);
	CHECK_STATUS(Corruption);
	EXPECT_TRUE(newVal.empty());
	if(s_.IsCorruption() && newVal.empty()) {
		log_success("key��member��Ӧ��score����,by�����ƣ�����10000000000000LL��");
	} else {
		log_fail("key��member��Ӧ��score����,by�����ƣ�����10000000000000LL��");
	}
}

TEST_F(NemoZSetTest, TestZRange) {
	log_message("\n========TestZRange========");
	string key, member;
	int64_t start, stop, num;
	vector<nemo::SM> sms;

#define ZRangeLoopProcess(sizeExpected, message)\
	sms.clear();\
	s_ = n_->ZRange(key, start, stop, sms);\
	CHECK_STATUS(OK);\
	EXPECT_EQ((sizeExpected), sms.size());\
	if(s_.ok() && sms.size() == (sizeExpected)) {\
		log_success(message);\
	} else {\
		log_fail(message);\
	}\
	do {} while(false)


	s_.OK();//key������/key���ڣ���û��zset�ṹ
	key = GetRandomKey_();
	member = GetRandomVal_();
	start = 0; 
	stop = GetRandomUint_(0, 256);
	ZRangeLoopProcess(0, "key����;0<end<start<len");

	num = 50;
	key = "ZRange_Test";
	write_zset_random_score(key, num);

	s_.OK();//key����;0<=start<end<=len-1
	start = GetRandomUint_(0, num-1);
	stop = GetRandomUint_(start, num-1);
	ZRangeLoopProcess(stop-start+1, "key����;0<=start<end<=len-1");

	s_.OK();//key����;-len<start<0;0<end<=len-1
	start = GetRandomUint_(0, num-1);
	stop = GetRandomUint_(start, num-1);
	start = start-num;
	ZRangeLoopProcess(stop-start-num+1, "key����;-len<start<0;0<end<=len-1");

	s_.OK();//key����;start<-len;0<end<=len-1
	start = (num+5)*(-1);
	stop = GetRandomUint_(0, num-1);
	ZRangeLoopProcess(stop+1, "key����;start<-len;0<end<=len-1");

	s_.OK();//key����;start>=len;0<end<=len-1
	start = num+11;
	stop = GetRandomUint_(0, num-1);
	ZRangeLoopProcess(0, "key����;start>=len;0<end<=len-1");

	s_.OK();//key����;0<start<=len-1;end<-len
	start = GetRandomUint_(0, num-1);
	stop = (-1)*(num+12);
	ZRangeLoopProcess(0, "key����;0<start<=len-1;end<-len");

	s_.OK();//key����;0<start<=len-1;-len<=end<0
	start = GetRandomUint_(0, num-1);
	stop = GetRandomUint_(start, num-1);
	stop = stop-num;
	ZRangeLoopProcess(stop+num-start+1, "key����;0<start<=len-1;-len<=end<0");

	s_.OK();//key����;0<start<=len-1;end>=len
	start = GetRandomUint_(0, num-1);
	stop = num+20;
	ZRangeLoopProcess(num-start, "key����;0<start<=len-1;end>=len");

	s_.OK();//key����;0<end<start<len
	stop = GetRandomUint_(0, num-2);
	start = GetRandomUint_(stop+1, num-1);
	ZRangeLoopProcess(0, "key����;0<end<start<len");
}

TEST_F(NemoZSetTest, TestZUnionStore) {
	log_message("\n========TestZUnionStore========");
	string destination, key1, key2, key3;
	int64_t res, zcard, num1, num2, num3;
	int num_keys;
	nemo::Aggregate agg;
	vector<string> keys;
	vector<double> weights;
	double scoreGet;

	s_.OK();//destination�����ڣ�numkeys=1��keysΪ����key��weightsĬ�ϣ�agg=sum��
	destination = GetRandomKey_();
	num_keys = 0;
	agg = SUM;
	res = 0;
	s_ = n_->ZUnionStore(destination, num_keys, keys, weights, agg, &res);
	CHECK_STATUS(OK);
	EXPECT_EQ(0, res);
	zcard = n_->ZCard(destination);
	EXPECT_EQ(0, zcard);
	if (s_.ok() && res == 0 && zcard == 0) {
		log_success("destination�����ڣ�numkeys=1��keysΪ����key��weightsĬ�ϣ�agg=sum��");
	} else {
		log_fail("destination�����ڣ�numkeys=1��keysΪ����key��weightsĬ�ϣ�agg=sum��");
	}

	s_.OK();//destination�����ڣ�numkeys=0��keysΪ����key��weightsĬ�ϣ�agg=sum��
	key1 = "ZUnionStoreSrcKey1";
	keys.push_back(key1);
	num1 = 10;
	write_zset_up_score_scope(key1, 0, 10);
	destination = "ZUnionStoreDstKey1";
	num_keys = 0;
	res = 0;
	s_ = n_->ZUnionStore(destination, num_keys, keys, weights, agg, &res);
	CHECK_STATUS(OK);
	EXPECT_EQ(0, res);
	zcard = n_->ZCard(destination);
	EXPECT_EQ(0, zcard);
	if (s_.ok() && res == 0 && zcard == 0) {
		log_success("destination�����ڣ�numkeys=0��keysΪ����key��weightsĬ�ϣ�agg=sum��");
	} else {
		log_fail("destination�����ڣ�numkeys=0��keysΪ����key��weightsĬ�ϣ�agg=sum��");
	}

	s_.OK();//destination�����ڣ�numkeys=1��keysΪ����key��weightsĬ�ϣ�agg=sum��
	num_keys = 1;
	res = 0;
	s_ = n_->ZUnionStore(destination, num_keys, keys, weights, agg, &res);
	CHECK_STATUS(OK);
	EXPECT_EQ(10, res);
	zcard = n_->ZCard(destination);
	EXPECT_EQ(10, zcard);
	if(s_.ok() && res == 10 && zcard == 10) {
		log_success("destination�����ڣ�numkeys=1��keysΪ����key��weightsĬ�ϣ�agg=sum��");
	} else {
		log_fail("destination�����ڣ�numkeys=1��keysΪ����key��weightsĬ�ϣ�agg=sum��");
	}

	/*
	s_.OK();//destination�����ڣ�numkeys>1��keysΪ����key��weightsĬ�ϣ�agg=sum��
	num_keys = 2;
	cout << "keys.size():" << keys.size() << endl;
	res = 0;
	s_ = n_->ZUnionStore(destination, num_keys, keys, weights, agg, &res);
	CHECK_STATUS(OK);
	zcard = n_->ZCard(destination);
	EXPECT_EQ(10, zcard);
	if (s_.ok() && res == 0 && zcard == 0) {
		log_success("destination�����ڣ�numkeys>1��keysΪ����key��weightsĬ�ϣ�agg=sum��");
	} else {
		log_fail("destination�����ڣ�numkeys>1��keysΪ����key��weightsĬ�ϣ�agg=sum��");
	}
	*/

	s_.OK();//destination�����ڣ�numkeys=size��keysΪ���key��û���ظ�Ԫ�أ�weightsĬ�ϣ�agg=sum��
	key1 = "ZUnionStoreSrcKey1";
	write_zset_up_score_scope(key1, 0, 20);
	key2 = "ZUnionStoreSrcKey2";
	write_zset_up_score_scope(key2, 20, 40);
	keys.clear();
	keys.push_back(key1);
	keys.push_back(key2);
	destination = "ZUnionStoreDstKey2";
	num_keys = 2;
	res = 0;
	s_ = n_->ZUnionStore(destination, num_keys, keys, weights, agg, &res);
	CHECK_STATUS(OK);
	EXPECT_EQ(40, res);
	if (s_.ok() && res == 40) {
		log_success("destination�����ڣ�numkeys=size��keysΪ���key��û���ظ�Ԫ�أ�weightsĬ�ϣ�agg=sum��");
	} else {
		log_fail("destination�����ڣ�numkeys=size��keysΪ���key��û���ظ�Ԫ�أ�weightsĬ�ϣ�agg=sum��");
	}

	s_.OK();//destination�����ڣ�numkeys=size��keysΪ���key�����ظ�Ԫ�أ�weightsĬ�ϣ�agg=sum���ж�sum�Ƿ��غ�������
	keys.clear();
	key1 = "ZUnionStoreSrcKey1";
	write_zset_up_score_scope(key1, 0, 20);
	key2 = "ZUnionStoreSrcKey2";
	write_zset_up_score_scope(key2, 19, 39);
	keys.push_back(key1);
	keys.push_back(key2);
	destination = "ZUnionStoreDstKey3";
	num_keys = 2;
	s_ = n_->ZUnionStore(destination, num_keys, keys, weights, agg, &res);
	CHECK_STATUS(OK);
	EXPECT_EQ(39, res);
	n_->ZScore(destination, itoa(19), &scoreGet);
	EXPECT_TRUE(isDoubleEqual(2*19, scoreGet));
	if (s_.ok() && res == 39 && isDoubleEqual(2*19, scoreGet)) {
		log_success("destination�����ڣ�numkeys=size��keysΪ���key�����ظ�Ԫ�أ�weightsĬ�ϣ�agg=sum���ж�sum�Ƿ��غ�������");
	} else {
		log_fail("destination�����ڣ�numkeys=size��keysΪ���key�����ظ�Ԫ�أ�weightsĬ�ϣ�agg=sum���ж�sum�Ƿ��غ�������");
	}

	s_.OK();//destination�����ڣ�numkeys=size��keysΪ���key�����ظ�Ԫ�أ�weightsĬ�ϣ�agg=max���ж�max�Ƿ��غ�������
	agg = MAX;
	string temp;
	n_->ZIncrby(key2, itoa(19), 10.0, temp);
	destination = "ZUnionStoreDstKey4";
	s_ = n_->ZUnionStore(destination, num_keys, keys, weights, agg, &res);
	CHECK_STATUS(OK);
	EXPECT_EQ(39, res);
	n_->ZScore(destination, itoa(19), &scoreGet);
	EXPECT_TRUE(isDoubleEqual(29.0, scoreGet));
	if (s_.ok() && res == 39 && isDoubleEqual(29.0, scoreGet)) {
		log_success("destination�����ڣ�numkeys=size��keysΪ���key�����ظ�Ԫ�أ�weightsĬ�ϣ�agg=max���ж�max�Ƿ��غ�������");
	} else {
		log_fail("destination�����ڣ�numkeys=size��keysΪ���key�����ظ�Ԫ�أ�weightsĬ�ϣ�agg=max���ж�max�Ƿ��غ�������");
	}

	s_.OK();//destination�����ڣ�numkeys=size��keysΪ���key�����ظ�Ԫ�أ�weightsĬ�ϣ�agg=max���ж�min�Ƿ��غ�������
	agg = MIN;
	destination = "ZUnionStoreDstKey5";
	s_ = n_->ZUnionStore(destination, num_keys, keys, weights, agg, &res);
	CHECK_STATUS(OK);
	EXPECT_EQ(39, res);
	n_->ZScore(destination, itoa(19), &scoreGet);
	EXPECT_TRUE(isDoubleEqual(19.0, scoreGet));
	if(s_.ok() && res == 39 && isDoubleEqual(19.0, scoreGet)) {
		log_success("destination�����ڣ�numkeys=size��keysΪ���key�����ظ�Ԫ�أ�weightsĬ�ϣ�agg=max���ж�min�Ƿ��غ�������");
	} else {
		log_fail("destination�����ڣ�numkeys=size��keysΪ���key�����ظ�Ԫ�أ�weightsĬ�ϣ�agg=max���ж�min�Ƿ��غ�������");
	}

	s_.OK();//destination���ڣ�numkeys=1��keysΪ����key��weightsĬ�ϣ�agg=sum����destination��ԭ�������Ƿ����
	destination = "ZUnionStoreDstKey6";
	write_zset_up_score_scope(destination, 0, 10);
	keys.clear();
	key1 = "ZUnionStoreSrcKey1";
	write_zset_up_score_scope(key1, 10, 20);
	keys.push_back(key1);
	num_keys = 1;
	s_ = n_->ZUnionStore(destination, num_keys, keys, weights, agg, &res);
	CHECK_STATUS(OK);
	EXPECT_EQ(10, res);
	nemo::Status s_tmp = n_->ZScore(destination, itoa(1), &scoreGet);
	EXPECT_TRUE(s_tmp.IsNotFound());
	if (s_.ok() && res == 10 && s_tmp.IsNotFound()) {
		log_success("destination���ڣ�numkeys=1��keysΪ����key��weightsĬ�ϣ�agg=sum����destination��ԭ�������Ƿ����");
	} else {
		log_fail("destination���ڣ�numkeys=1��keysΪ����key��weightsĬ�ϣ�agg=sum����destination��ԭ�������Ƿ����");
	}
}

TEST_F(NemoZSetTest, TestZInterStore) {
	log_message("\n========TestZInterStore========");
	string destination, key1, key2, key3;
	int64_t res, zcard, num1, num2, num3;
	int num_keys;
	nemo::Aggregate agg;
	vector<string> keys;
	vector<double> weights;
	double scoreGet;

	s_.OK();//destination�����ڣ�numkeys=1��keysΪ����key��weightsĬ�ϣ�agg=sum��
	destination =  "ZInterStore_Test_Dst1";
	keys.clear();
	key1 = "ZInterStore_Test_Src1";
	keys.push_back(key1);
	write_zset_up_score_scope(key1, 0, 10);
	num_keys = 1;
	agg = SUM;
	res = 0;
	s_ = n_->ZInterStore(destination, num_keys, keys, weights, agg, &res);
	CHECK_STATUS(OK);
	EXPECT_EQ(0, res);
	if (s_.ok() && res == 0) {
		log_success("destination�����ڣ�numkeys=1��keysΪ����key��weightsĬ�ϣ�agg=sum��");
	} else {
		log_fail("destination�����ڣ�numkeys=1��keysΪ����key��weightsĬ�ϣ�agg=sum��");
	}

	s_.OK();//destination�����ڣ�keysΪ����key��û���ظ�Ԫ�أ�weightsĬ�ϣ�agg=sum��
	destination = "ZInterStore_Test_Dst2";
	keys.clear();
	num_keys = 2;
	key1 = "ZInterStore_Test_Src2";
	write_zset_up_score_scope(key1, 0, 10);
	keys.push_back(key1);
	key2 = "ZInterStore_Test_Src3";
	write_zset_up_score_scope(key2, 10, 20);
	keys.push_back(key2);
	agg = SUM;
	res = -1;
	s_ = n_->ZInterStore(destination, num_keys, keys, weights, agg, &res);
	CHECK_STATUS(OK);
	EXPECT_EQ(0, res);
	if (s_.ok() && res == 0) {
		log_success("destination�����ڣ�keysΪ����key��û���ظ�Ԫ�أ�weightsĬ�ϣ�agg=sum��");
	} else {
		log_fail("destination�����ڣ�keysΪ����key��û���ظ�Ԫ�أ�weightsĬ�ϣ�agg=sum��");
	}

	s_.OK();//destination�����ڣ�keysΪ����key�����ظ�Ԫ�أ�weightsĬ�ϣ�agg=sum��
	destination = "ZInterStore_Test_Dst3";
	keys.clear();
	num_keys = 2;
	key1 = "ZInterStore_Test_Src4";
	write_zset_up_score_scope(key1, 0, 10);
	keys.push_back(key1);
	key2 = "ZInterStore_Test_Src5";
	write_zset_up_score_scope(key2, 9, 19);
	keys.push_back(key2);
	agg = SUM;
	res = -1;
	s_ = n_->ZInterStore(destination, num_keys, keys, weights, agg, &res);
	CHECK_STATUS(OK);
	EXPECT_EQ(1, res);
	n_->ZScore(destination, itoa(9), &scoreGet);
	EXPECT_TRUE(isDoubleEqual(2*9.0,scoreGet));
	if (s_.ok() && res == 1 && isDoubleEqual(2*9.0,scoreGet)) {
		log_success("destination�����ڣ�keysΪ����key�����ظ�Ԫ�أ�weightsĬ�ϣ�agg=sum");
	} else {
		log_fail("destination�����ڣ�keysΪ����key�����ظ�Ԫ�أ�weightsĬ�ϣ�agg=sum");
	}

	s_.OK();//destination�����ڣ�keysΪ����key��û���ظ�Ԫ�أ�weightsĬ�ϣ�agg=sum��
	destination = "ZInterStore_Test_Dst4";
	keys.clear();
	num_keys = 3;
	key1 = "ZInterStore_Test_Src6";
	write_zset_up_score_scope(key1, 0, 10);
	keys.push_back(key1);
	key2 = "ZInterStore_Test_Src7";
	write_zset_up_score_scope(key2, 10, 20);
	keys.push_back(key2);
	key3 = "ZInterStore_Test_Src8";
	write_zset_up_score_scope(key3, 20, 30);
	keys.push_back(key3);
	agg = SUM;
	res = -1;
	s_ = n_->ZInterStore(destination, num_keys, keys, weights, agg, &res);
	CHECK_STATUS(OK);
	EXPECT_EQ(0, res);
	if (s_.ok() && 0 == res) {
		log_success("destination�����ڣ�keysΪ����key��û���ظ�Ԫ�أ�weightsĬ�ϣ�agg=sum��");
	} else {
		log_fail("destination�����ڣ�keysΪ����key��û���ظ�Ԫ�أ�weightsĬ�ϣ�agg=sum��");
	}

	s_.OK();//destination�����ڣ�keysΪ����key��ֻ�������ظ�Ԫ�أ�û��ȫ���ظ���Ԫ�أ�weightsĬ�ϣ�agg=sum��
	destination = "ZInterStore_Test_Dst5";
	keys.clear();
	num_keys = 3;
	key1 = "ZInterStore_Test_Src9";
	write_zset_up_score_scope(key1, 0, 30);
	keys.push_back(key1);
	key2 = "ZInterStore_Test_Src10";
	write_zset_up_score_scope(key2, 20, 50);
	keys.push_back(key2);
	key3 = "ZInterStore_Test_Src11";
	write_zset_up_score_scope(key3, 40, 70);
	keys.push_back(key3);
	agg = SUM;
	res = -1;
	s_ = n_->ZInterStore(destination, num_keys, keys, weights, agg, &res);
	CHECK_STATUS(OK);
	EXPECT_EQ(0, res);
	if (s_.ok() && 0 == res) {
		log_success("destination�����ڣ�keysΪ����key��ֻ�������ظ�Ԫ�أ�û��ȫ���ظ���Ԫ�أ�weightsĬ�ϣ�agg=sum��");
	} else {
		log_fail("destination�����ڣ�keysΪ����key��ֻ�������ظ�Ԫ�أ�û��ȫ���ظ���Ԫ�أ�weightsĬ�ϣ�agg=sum��");
	}

	s_.OK();//destination�����ڣ�keysΪ����key����ȫ���ظ���Ԫ�أ�weightsĬ�ϣ�agg=sum��
	destination = "ZInterStore_Test_Dst6";
	keys.clear();
	num_keys = 3;
	key1 = "ZInterStore_Test_Src12";
	write_zset_up_score_scope(key1, 0, 30);
	keys.push_back(key1);
	key2 = "ZInterStore_Test_Src13";
	write_zset_up_score_scope(key2, 20, 50);
	keys.push_back(key2);
	key3 = "ZInterStore_Test_Src14";
	write_zset_up_score_scope(key3, 29, 70);
	keys.push_back(key3);
	agg = SUM;
	res = -1;
	s_ = n_->ZInterStore(destination, num_keys, keys, weights, agg, &res);
	CHECK_STATUS(OK);
	EXPECT_EQ(1, res);
	n_->ZScore(destination, itoa(29), &scoreGet);
	EXPECT_TRUE(isDoubleEqual(3*29.0, scoreGet));
	if (s_.ok() && 1 == res && isDoubleEqual(3*29.0, scoreGet)) {
		log_success("destination�����ڣ�keysΪ����key����ȫ���ظ���Ԫ�أ�weightsĬ�ϣ�agg=sum��");
	} else {
		log_fail("destination�����ڣ�keysΪ����key����ȫ���ظ���Ԫ�أ�weightsĬ�ϣ�agg=sum��");
	}
	
	s_.OK();//destination�����ڣ�keysΪ����key�����ظ�Ԫ�أ�weightsĬ�ϣ�agg=max��
	destination = "ZInterStore_Test_Dst7";
	keys.clear();
	num_keys = 2;
	key1 = "ZInterStore_Test_Src15";
	write_zset_up_score_scope(key1, 0, 10);
	keys.push_back(key1);
	key2 = "ZInterStore_Test_Src16";
	write_zset_up_score_scope(key2, 9, 40);
	string temp;
	n_->ZIncrby(key2, itoa(9), 11, temp);
	keys.push_back(key2);
	agg = MAX;
	res = -1;
	s_ = n_->ZInterStore(destination, num_keys, keys, weights, agg, &res);
	CHECK_STATUS(OK);
	EXPECT_EQ(1, res);
	n_->ZScore(destination, itoa(9), &scoreGet);
	EXPECT_TRUE(isDoubleEqual(20.0, scoreGet));
	if (s_.ok() && 1 == res && isDoubleEqual(20.0, scoreGet)) {
		log_success("destination�����ڣ�keysΪ����key�����ظ�Ԫ�أ�weightsĬ�ϣ�agg=max");
	} else {
		log_fail("destination�����ڣ�keysΪ����key�����ظ�Ԫ�أ�weightsĬ�ϣ�agg=max");
	}

	s_.OK();//destination�����ڣ�keysΪ����key�����ظ�Ԫ�أ�weightsĬ�ϣ�agg=min
	agg = MIN;
	res = -1;
	s_ = n_->ZInterStore(destination, num_keys, keys, weights, agg, &res);
	CHECK_STATUS(OK);
	EXPECT_EQ(1, res);
	n_->ZScore(destination, itoa(9), &scoreGet);
	EXPECT_TRUE(isDoubleEqual(9.0, scoreGet));
	if (s_.ok() && 1 == res && isDoubleEqual(9.0, scoreGet)) {
		log_success("destination�����ڣ�keysΪ����key�����ظ�Ԫ�أ�weightsĬ�ϣ�agg=min");
	} else {
		log_fail("destination�����ڣ�keysΪ����key�����ظ�Ԫ�أ�weightsĬ�ϣ�agg=min");
	}

	s_.OK();//destination���ڣ�keysΪ����key��û���ظ�Ԫ�أ�weightsĬ�ϣ�agg=sum��
	destination = "ZInterStore_Test_Dst8";
	write_zset_up_score_scope(destination, 0, 10);
	keys.clear();
	num_keys = 2;
	key1 = "ZInterStore_Test_Src17";
	write_zset_up_score_scope(key1, 10, 20);
	keys.push_back(key1);
	key2 = "ZInterStore_Test_Src18";
	write_zset_up_score_scope(key2, 20, 30);
	keys.push_back(key2);
	agg = SUM;
	res = -1;
	s_ = n_->ZInterStore(destination, num_keys, keys, weights, agg, &res);
	CHECK_STATUS(OK);
	EXPECT_EQ(0, res);
	nemo::Status s_temp = n_->ZScore(destination, itoa(1), &scoreGet);
	EXPECT_TRUE(s_temp.IsNotFound());
	if (s_.ok() && res == 0 && s_temp.IsNotFound()) {
		log_success("destination���ڣ�keysΪ����key��û���ظ�Ԫ�أ�weightsĬ�ϣ�agg=sum");
	} else {
		log_fail("destination���ڣ�keysΪ����key��û���ظ�Ԫ�أ�weightsĬ�ϣ�agg=sum");
	}
}

TEST_F(NemoZSetTest, TestZRangebyscore) {
	log_message("\n========TestZRangebyscore========");
	string key;
	double start, stop, offset;
	int64_t num;
	vector<nemo::SM> sms;

	key = "ZRangebyscore_Test";
	num = 100;
	write_zset_up_score(key, num);

#define ZRangebyscoreLoopProcess(sizeExpected, message)\
	sms.clear();\
	s_ = n_->ZRangebyscore(key, start, stop, sms);\
	CHECK_STATUS(OK);\
	EXPECT_EQ((sizeExpected), sms.size());\
	if (s_.ok() && sms.size() == (sizeExpected)) {\
		log_success(message);\
	} else {\
		log_fail(message);\
	}\
	do {} while(false)

	s_.OK();//scoreMin<start<end<scoreMax
	start = GetRandomFloat_(0, num-2);
	stop = GetRandomFloat_(ceil(start), num-1);
	ZRangebyscoreLoopProcess(floor(stop)-ceil(start)+1, "scoreMin<start<end<scoreMax");

	s_.OK();//scoreMin<end<start<scoreMax
	stop = GetRandomFloat_(0, num-3);
	start = GetRandomFloat_(ceil(stop)+1, num-1);
	ZRangebyscoreLoopProcess(0, "scoreMin<end<start<scoreMax");

	s_.OK();//start<scoreMin��end��scores��Χ��
	start = -1;
	stop = GetRandomFloat_(0, num-1);
	ZRangebyscoreLoopProcess(floor(stop)+1, "start<scoreMin��end��scores��Χ��");

	s_.OK();//start��scores��Χ�ڣ�endΪ>scoreMax
	start = GetRandomFloat_(0, num-1);
	stop = num+1;
	ZRangebyscoreLoopProcess(num-ceil(start), "start��scores��Χ�ڣ�endΪ>scoreMax,");

	s_.OK();//start,end��>scoreMax,��start>end
	start = num+10;
	stop = num+5;
	ZRangebyscoreLoopProcess(0, "start,end��>scoreMax,��start>end");

	s_.OK();//start,end��>scoreMax��start<end
	start = num+6;
	stop = num+23;
	ZRangebyscoreLoopProcess(0, "start,end��>scoreMax��start<end");

	s_.OK();//start,end��<scoreMin��start<end
	start = -10;
	stop = -1;
	ZRangebyscoreLoopProcess(0, "start,end��<scoreMin��start<end");

	s_.OK();//start,end��<scoreMin��start>end
	start = -2;
	stop = -23;
	ZRangebyscoreLoopProcess(0, "start,end��<scoreMin��start>end");

	s_.OK();//start<scoreMin<scoreMax<end
	start = -2;
	stop = num+1;
	ZRangebyscoreLoopProcess(num, "start<scoreMin<scoreMax<end");

	s_.OK();//end<scoreMin<scoreMax<start
	stop = -3;
	start = num+10;
	ZRangebyscoreLoopProcess(0, "end<scoreMin<scoreMax<start");

	s_.OK();//start=scoreMin��end=scoreMax��0<offset<len
	start = 0;
	stop = num-1;
	offset = GetRandomUint_(0, num-1);
	sms.clear();
	s_ = n_->ZRangebyscore(key, start, stop, sms, offset);
	CHECK_STATUS(OK);
	EXPECT_EQ(num-offset, sms.size());
	if (s_.ok() && sms.size() == num-offset) {
		log_success("start=scoreMin��end=scoreMax��0<offset<len");
	} else {
		log_fail("start=scoreMin��end=scoreMax��0<offset<len");
	}
}

TEST_F(NemoZSetTest, TestZRem) {
	log_message("\n========TestZRem========");
	string key, member;
	double score;
	int64_t res;

	s_.OK();//ԭ����key������
	key = GetRandomKey_();
	member = GetRandomVal_();
	res = -1;
	s_ = n_->ZRem(key, member, &res);
	CHECK_STATUS(NotFound);
	EXPECT_EQ(0, res);
	if (s_.IsNotFound() && res == 0) {
		log_success("ԭ����key������");
	} else {
		log_fail("ԭ����key������");
	}

	s_.OK();//ԭ����key���ڣ���Ĳ���ZSet�ṹ����
	key = GetRandomKey_();
	member = GetRandomVal_();
	n_->Set(key, member);
	s_ = n_->ZRem(key, member, &res);
	CHECK_STATUS(NotFound);
	EXPECT_EQ(0,res);
	if (s_.IsNotFound() && res == 0) {
		log_success("ԭ����key���ڣ���Ĳ���ZSet�ṹ����");
	} else {
		log_fail("ԭ����key���ڣ���Ĳ���ZSet�ṹ����");
	}

	s_.OK();//ԭ����key���ڣ������ZSet�����ݽṹ��������member
	key = GetRandomKey_();
	member = GetRandomVal_();
	score = GetRandomFloat_();
	n_->ZAdd(key, score, member, &res);
	member = GetRandomVal_();
	s_ = n_->ZRem(key, member, &res);
	CHECK_STATUS(NotFound);
	EXPECT_EQ(0, res);
	if (s_.IsNotFound() && res == 0) {
		log_success("ԭ����key���ڣ������ZSet�����ݽṹ��������member");
	} else {
		log_fail("ԭ����key���ڣ������ZSet�����ݽṹ��������member");
	}

	s_.OK();//ԭ����key���ڣ������ZSet�����ݽṹ������member
	key = GetRandomKey_();
	member = GetRandomVal_();
	score = GetRandomFloat_();
	n_->ZAdd(key, score, member, &res);
	s_ = n_->ZRem(key, member, &res);
	CHECK_STATUS(OK);
	EXPECT_EQ(1, res);
	if (s_.ok() && res == 1) {
		log_success("ԭ����key���ڣ������ZSet�����ݽṹ������member");
	} else {
		log_fail("ԭ����key���ڣ������ZSet�����ݽṹ������member");
	}
}

TEST_F(NemoZSetTest, TestZRank) {
	log_message("\n========TestZRank========");
	string key, member;
	int64_t rank, num, memberInt;

	s_.OK();//ԭ����key������/ԭ����key���ڣ���Ĳ���zset�ṹ/ԭ����key�����zset�ṹ������û��member��Ա
	key = GetRandomKey_();
	member = GetRandomVal_();
	s_ = n_->ZRank(key, member, &rank);
	CHECK_STATUS(NotFound);
	EXPECT_EQ(0, rank);
	if (s_.IsNotFound() && rank == 0) {
		log_success("ԭ����key������/ԭ����key���ڣ���Ĳ���zset�ṹ/ԭ����key�����zset�ṹ������û��member��Ա");
	} else {
		log_fail("ԭ����key������/ԭ����key���ڣ���Ĳ���zset�ṹ/ԭ����key�����zset�ṹ������û��member��Ա");
	}

	s_.OK();//ԭ����key�����zset�ṹ��������member��Ա
	key = "ZRank_Test";
	num = 50;
	write_zset_up_score(key, num);
	memberInt = GetRandomUint_(0, num-1);
	s_ = n_->ZRank(key, itoa(memberInt), &rank);
	CHECK_STATUS(OK);
	EXPECT_EQ(memberInt, rank);
	if (s_.ok() && memberInt == rank) {
		log_success("ԭ����key�����zset�ṹ��������member��Ա");
	} else {
		log_fail("ԭ����key�����zset�ṹ��������member��Ա");
	}
}

TEST_F(NemoZSetTest, TestZRevrank) {
	log_message("\n========TestZRevrank========");
	string key, member;
	int64_t rank, num, memberInt;

	s_.OK();//ԭ����key������/ԭ����key���ڣ���Ĳ���zset�ṹ/ԭ����key�����set�ṹ������û��member��Ա
	key = GetRandomKey_();
	member = GetRandomVal_();
	s_ = n_->ZRevrank(key, member, &rank);
	CHECK_STATUS(NotFound);
	EXPECT_EQ(0, rank);
	if (s_.IsNotFound() && rank == 0) {
		log_success("ԭ����key������/ԭ����key���ڣ���Ĳ���zset�ṹ/ԭ����key�����zset�ṹ������û��member��Ա");
	} else {
		log_fail("ԭ����key������/ԭ����key���ڣ���Ĳ���zset�ṹ/ԭ����key�����zset�ṹ������û��member��Ա");
	}

	s_.OK();//ԭ����key�����set�ṹ��������member��Ա
	key = "ZRevrank_Test";
	num = 50;
	write_zset_up_score(key, num);
	memberInt = GetRandomUint_(0, num-1);
	s_ = n_->ZRevrank(key, itoa(memberInt), &rank);
	CHECK_STATUS(OK);
	EXPECT_EQ((num-1-memberInt), rank);
	if (s_.ok() && (num-1-memberInt) == rank) {
		log_success("ԭ����key�����zset�ṹ��������member��Ա");
	} else {
		log_fail("ԭ����key�����zset�ṹ��������member��Ա");
	}
}

TEST_F(NemoZSetTest, TestZScore) {
	log_message("\n========TestZScore========");
	string key, member;
	int64_t res;
	double score, scoreGet;

	s_.OK();//ԭ����key������
	key = GetRandomKey_();
	member = GetRandomVal_();
	s_ = n_->ZScore(key, member, &scoreGet);
	CHECK_STATUS(NotFound);
	EXPECT_EQ(0, scoreGet);
	if (s_.IsNotFound() && scoreGet == 0) {
		log_success("ԭ����key������");
	} else {
		log_fail("ԭ����key������");
	}

	s_.OK();//ԭ����key���ڣ���Ĳ���zset�ṹ
	key = GetRandomKey_();
	member = GetRandomVal_();
	n_->SAdd(key, member, &res);
	s_ = n_->ZScore(key, member, &scoreGet);
	CHECK_STATUS(NotFound);
	EXPECT_EQ(0, scoreGet);
	if (s_.IsNotFound() && scoreGet == 0) {
		log_success("ԭ����key���ڣ���Ĳ���zset�ṹ");
	} else {
		log_fail("ԭ����key���ڣ���Ĳ���zset�ṹ");
	}
	
	s_.OK();//ԭ����key�����zset�ṹ,������member��Ա
	key = GetRandomKey_();
	member = GetRandomVal_();
	score = GetRandomFloat_();
	n_->ZAdd(key, score, member, &res);
	s_ = n_->ZScore(key, member, &scoreGet);
	CHECK_STATUS(OK);
	EXPECT_EQ(score, scoreGet);
	if (s_.ok() && score == scoreGet) {
		log_success("ԭ����key�����zset�ṹ,������member��Ա");
	} else {
		log_fail("ԭ����key�����zset�ṹ,������member��Ա");
	}

	s_.OK();//ԭ����key�����set�ṹ������û��member��Ա
	member = GetRandomVal_();
	s_ = n_->ZScore(key, member, &scoreGet);
	CHECK_STATUS(NotFound);
	EXPECT_EQ(0, scoreGet);
	if (s_.IsNotFound() && 0 == scoreGet) {
		log_success("ԭ����key�����set�ṹ������û��member��Ա");
	} else {
		log_fail("ԭ����key�����set�ṹ������û��member��Ա");
	}
}

TEST_F(NemoZSetTest, TestZRangelex) {
	log_message("\n========TestZRangelex========");
	string key, member, min, max;
	uint32_t minInt, maxInt, num;
	vector<string> members;

	
	s_.OK();//ԭ����key������/ԭ����key���ڣ���Ĳ���set�ṹ
	key = GetRandomKey_();
	min = "";
	max = "";
	members.clear();
	s_ = n_->ZRangebylex(key, min, max, members);
	CHECK_STATUS(OK);
	EXPECT_TRUE(member.empty());
	if (s_.ok() && members.empty()) {
		log_success("ԭ����key������/ԭ����key���ڣ���Ĳ���set�ṹ");
	} else {
		log_fail("ԭ����key������/ԭ����key���ڣ���Ĳ���set�ṹ");
	}

	key = "ZRangebylex_Test";
	num = 10;
	write_zset_up_score(key, num);

	s_.OK();//ԭ����key���ڣ�max��min��membersû�н���
	min = "a";
	max = "z";
	members.clear();
	s_ = n_->ZRangebylex(key, min, max, members);
	CHECK_STATUS(OK);
	EXPECT_TRUE(members.empty());
	if (s_.ok() && members.empty()) {
		log_success("ԭ����key���ڣ�max��min��membersû�н���");
	} else {
		log_fail("ԭ����key���ڣ�max��min��membersû�н���");
	}

	s_.OK();//ԭ����key���ڣ�max��min��members�н�������û����ͬ��score
	minInt = GetRandomUint_(0, num-2);
	maxInt = GetRandomUint_(minInt+1, num-1);
	members.clear();
	s_ = n_->ZRangebylex(key, itoa(minInt), itoa(maxInt), members);
	CHECK_STATUS(OK);
	EXPECT_EQ(maxInt-minInt+1, members.size());
	if (s_.ok() && maxInt-minInt+1==members.size()) {
		log_success("ԭ����key���ڣ�max��min��members�н�������û����ͬ��score");
	} else {
		log_fail("ԭ����key���ڣ�max��min��members�н�������û����ͬ��score");
	}

	s_.OK();//ԭ����key���ڣ�max��minλ��
	min = "";
	max = "";
	members.clear();
	s_ = n_->ZRangebylex(key, min, max, members);
	CHECK_STATUS(OK);
	EXPECT_EQ(num, members.size());
	if (s_.ok() && num == members.size()) {
		log_success("ԭ����key���ڣ�max��min��members�н�������offset>0");
	} else {
		log_fail("ԭ����key���ڣ�max��min��members�н�������offset>0");
	}

	s_.OK();//ԭ����key���ڣ�max��min��scores�н�������offset>0
	min = "";
	max = "";
//	uint32_t offset = GetRandomUint_(0, num-1);	
//	members.clear();
//	s_ = n_->ZRangebylex(key, min, max, members, offset);
//	CHECK_STATUS(OK);
//	EXPECT_EQ(num-offset, members.size());
//	if (s_.ok() && num-offset == members.size()) {
//		log_success("ԭ����key���ڣ�max��min��members�н�������offset>0");
//	} else {
//		log_fail("ԭ����key���ڣ�max��min��members�н�������offset>0");
//	}
	
	s_.OK();//ԭ����key���ڣ�max��min��scores�н�����������ͬ��score
	min = "";
	max = "";
	write_zset_same_score(key, num);
	members.clear();
	s_ = n_->ZRangebylex(key, min, max, members);
	CHECK_STATUS(OK);
	EXPECT_EQ(num, members.size());
	EXPECT_TRUE(isSorted(members));
	if (s_.ok() && num == members.size() && isSorted(members)) {
		log_success("ԭ����key���ڣ�max��min��members�н�����������ͬ��score");
	} else {
		log_fail("ԭ����key���ڣ�max��min��members�н�����������ͬ��score");
	}
}

TEST_F(NemoZSetTest, TestZLexcount) {
	log_message("\n========TestZLexcount========");
	string key, min, max;
	int64_t count, num, numPre, minInt, maxInt;

	s_.OK();//ԭ����key������/ԭ����key���ڣ���Ĳ���set�ṹ
	key = GetRandomKey_();
	min = "";
	max = "";
	count = -1;
	s_ = n_->ZLexcount(key, min, max, &count);
	CHECK_STATUS(OK);
	EXPECT_EQ(0, count);
	if (s_.ok() && 0 == count) {
		log_success("ԭ����key������/ԭ����key���ڣ���Ĳ���set�ṹ");
	} else {
		log_fail("ԭ����key������/ԭ����key���ڣ���Ĳ���set�ṹ");
	}

	numPre = 10000;
	num = 100;
	key = "ZLexcount_Test";
	write_zset_random_score(key, num, numPre);

	s_.OK();//ԭ����key���ڣ�max��min��membersû�н���
	minInt = num + 10;
	maxInt = num + 23;
	min = itoa(minInt + numPre);
	max = itoa(maxInt + numPre);
	count = -1;
	s_ = n_->ZLexcount(key, min, max, &count);
	CHECK_STATUS(OK);
	EXPECT_EQ(0, count);
	if (s_.ok() && count == 0) {
		log_success("ԭ����key���ڣ�max��min��membersû�н���");
	} else {
		log_fail("ԭ����key���ڣ�max��min��membersû�н���");
	}

	s_.OK();//ԭ����key���ڣ�max��min��members�н���
	minInt = GetRandomUint_(0, num-2);
	maxInt = GetRandomUint_(0, num-1);
	min = itoa(minInt + numPre);
	max = itoa(maxInt + numPre);
	count = -1;
	s_ = n_->ZLexcount(key, min, max, &count);
	CHECK_STATUS(OK);
	EXPECT_EQ(maxInt-minInt+1, count);
	if (s_.ok() && count == maxInt-minInt+1) {
		log_success("ԭ����key���ڣ�max��min��members�н���");
	} else {
		log_fail("ԭ����key���ڣ�max��min��members�н���");
	}

	s_.OK();//ԭ����key���ڣ�max��min��Ϊ��
	min = "";
	max = "";
	count = -1;
	s_ = n_->ZLexcount(key, min, max, &count);
	CHECK_STATUS(OK);
	EXPECT_EQ(num, count);
	if (s_.ok() && count == num) {
		log_success("ԭ����key���ڣ�max��min��Ϊ��");
	} else {
		log_fail("ԭ����key���ڣ�max��min��Ϊ��");
	}
}

TEST_F(NemoZSetTest, TestZRemrangebylex) {
	log_message("\n========TestZRemrangbylex========");
	string key, min, max;
	int64_t count, minInt, maxInt, num, numPre;

	s_.OK();//ԭ����key������/ԭ����key���ڣ���Ĳ���set�ṹ
	key = GetRandomKey_();
	min = "";
	max = "";
	count = -1;
	s_ = n_->ZRemrangebylex(key, min, max, false, false, &count);
	CHECK_STATUS(OK);
	EXPECT_EQ(0, count);
	if (s_.ok() && 0 == count) {
		log_success("ԭ����key������/ԭ����key���ڣ���Ĳ���set�ṹ");
	} else {
		log_fail("ԭ����key������/ԭ����key���ڣ���Ĳ���set�ṹ");
	}

	numPre = 10000;
	num = 100;
	key = "ZLexcount_Test";
	write_zset_random_score(key, num, numPre);

	s_.OK();//ԭ����key���ڣ�max��min��membersû�н���
	minInt = num + 23;
	maxInt = num + 43;
	min = itoa(minInt + numPre);
	max = itoa(maxInt + numPre);
	count = -1;
	s_ = n_->ZRemrangebylex(key, min, max, false, false, &count);
	CHECK_STATUS(OK);
	EXPECT_EQ(0, count);
	if (s_.ok() && count == 0) {
		log_success("ԭ����key���ڣ�max��min��membersû�н���");
	} else {
		log_fail("ԭ����key���ڣ�max��min��membersû�н���");
	}

	s_.OK();//ԭ����key���ڣ�max��min��members�н���
	minInt = GetRandomUint_(0, num-2);
	maxInt = GetRandomUint_(minInt+1, num-1);
	min = itoa(minInt + numPre);
	max = itoa(maxInt + numPre);
	count = -1;
	s_ = n_->ZRemrangebylex(key, min, max, false, false, &count);
	CHECK_STATUS(OK);
	EXPECT_EQ(maxInt-minInt+1, count);
	if (s_.ok() && count == maxInt-minInt+1) {
		log_success("ԭ����key���ڣ�max��min��members�н���");
	} else {
		log_fail("ԭ����key���ڣ�max��min��members�н���");
	}


	s_.OK();//ԭ����key���ڣ�max��min��Ϊ��
	write_zset_random_score(key, num, numPre);
	min = "";
	max = "";
	count = -1;
	s_ = n_->ZRemrangebylex(key, min, max, false, false, &count);
	CHECK_STATUS(OK);
	EXPECT_EQ(num, count);
	if (s_.ok() && count == num) {
		log_success("ԭ����key���ڣ�max��min��Ϊ��");
	} else {
		log_fail("ԭ����key���ڣ�max��min��Ϊ��");
	}
}

TEST_F(NemoZSetTest, TestZRemrangebyrank) {
	log_message("\n========TestZRemrangebyrank========");
	string key;
	int64_t start, stop, count, num;

	s_.OK();//ԭ����key������/ԭ����key���ڣ���Ĳ���set�ṹ
	key = GetRandomKey_();
	start = 0;
	stop = 100;
	count = -1;
	s_ = n_->ZRemrangebyrank(key, start, stop, &count);
	CHECK_STATUS(OK);
	EXPECT_EQ(0, count);
	if (s_.ok() && count == 0) {
		log_success("ԭ����key������/ԭ����key���ڣ���Ĳ���set�ṹ");
	} else {
		log_fail("ԭ����key������/ԭ����key���ڣ���Ĳ���set�ṹ");
	}

	key = "ZRemrangebyrank_Test";
	num = 100;
	write_zset_up_score(key, num);
	s_.OK();//key����;0<=start<end<=len-1
	start = GetRandomUint_(0, num-1);
	stop = GetRandomUint_(start, num-1);
	count = -1;
	s_ = n_->ZRemrangebyrank(key, start, stop, &count);
	CHECK_STATUS(OK);
	EXPECT_EQ(stop-start+1, count);
	if(s_.ok() && count == stop-start+1) {
		log_success("key����;0<=start<end<=len-1");
	} else {
		log_fail("key����;0<=start<end<=len-1");
	}

	write_zset_up_score(key, num);
	s_.OK();//key����;-len<start<0;0<end<=len-1
	start = GetRandomUint_(0, num-1);
	stop = GetRandomUint_(start, num-1);
	start = start-num;
	count = -1;
	s_ = n_->ZRemrangebyrank(key, start, stop, &count);
	CHECK_STATUS(OK);
	EXPECT_EQ(stop-start-num+1, count);
	if(s_.ok() && count == stop-start-num+1) {
		log_success("key����;-len<start<0;0<end<=len-1");
	} else {
		log_fail("key����;-len<start<0;0<end<=len-1");
	}

	write_zset_up_score(key, num);
	s_.OK();//key����;start<-len;0<end<=len-1
	start = (-1)*(num+4);
	stop = GetRandomUint_(0, num-1);
	count = -1;
	s_ = n_->ZRemrangebyrank(key, start, stop, &count);
	CHECK_STATUS(OK);
	EXPECT_EQ(stop+1, count);
	if(s_.ok() && count == stop+1) {
		log_success("key����;start<-len;0<end<=len-1");
	} else {
		log_fail("key����;start<-len;0<end<=len-1");
	}

	write_zset_up_score(key, num);
	s_.OK();//key����;start>=len;0<end<=len-1
	start = num+23;
	stop = GetRandomUint_(start, num-1);
	count = -1;
	s_ = n_->ZRemrangebyrank(key, start, stop, &count);
	CHECK_STATUS(OK);
	EXPECT_EQ(0, count);
	if(s_.ok() && count == 0) {
		log_success("key����;start>=len;0<end<=len-1");
	} else {
		log_fail("key����;start>=len;0<end<=len-1");
	}

	write_zset_up_score(key, num);
	s_.OK();//key����;0<start<=len-1;end<-len
	start = GetRandomUint_(0, num-1);
	stop = (-1)*(num+12);
	count = -1;
	s_ = n_->ZRemrangebyrank(key, start, stop, &count);
	CHECK_STATUS(OK);
	EXPECT_EQ(0, count);
	if(s_.ok() && count == 0) {
		log_success("key����;0<start<=len-1;end<-len");
	} else {
		log_fail("key����;0<start<=len-1;end<-len");
	}
	
	write_zset_up_score(key, num);
	s_.OK();//key����;0<start<=len-1;-len<=end<0
	start = GetRandomUint_(0, num-1);
	stop = GetRandomUint_(start, num-1);
	stop = stop - num;
	count = -1;
	s_ = n_->ZRemrangebyrank(key, start, stop, &count);
	CHECK_STATUS(OK);
	EXPECT_EQ(stop+num-start+1, count);
	if(s_.ok() && count == stop+num-start+1) {
		log_success("key����;0<start<=len-1;-len<=end<0");
	} else {
		log_fail("key����;0<start<=len-1;-len<=end<0");
	}

	write_zset_up_score(key, num);
	s_.OK();//key����;0<start<=len-1;end>=len
	start = GetRandomUint_(0, num-1);
	stop = num+17;
	count = -1;
	s_ = n_->ZRemrangebyrank(key, start, stop, &count);
	CHECK_STATUS(OK);
	EXPECT_EQ(num-start, count);
	if(s_.ok() && count == num-start) {
		log_success("key����;0<start<=len-1;end>=len");
	} else {
		log_fail("key����;0<start<=len-1;end>=len");
	}

	write_zset_up_score(key, num);
	s_.OK();//key����;0<end<start<len
	stop = GetRandomUint_(0, num-2);
	start = GetRandomUint_(stop+1, num-1);
	count = -1;
	s_ = n_->ZRemrangebyrank(key, start, stop, &count);
	CHECK_STATUS(OK);
	EXPECT_EQ(0, count);
	if(s_.ok() && count == 0) {
		log_success("key����;0<end<start<len");
	} else {
		log_fail("key����;0<end<start<len");
	}
}

TEST_F(NemoZSetTest, TestZRemrangebyscore) {
	log_message("\n========TestZRemrangebyscore========");

	string key;
	double start, stop;
	int64_t count, num;

	key = "ZRemrangebyrank_Test";
	num = 100;

	write_zset_up_score(key, num);
	s_.OK();//key�ǿգ�scoreMin<start<end<scoreMax
	start = GetRandomFloat_(0, num-2);
	stop = GetRandomFloat_(ceil(start), num-1);
	count = -1;
	s_ = n_->ZRemrangebyscore(key, start, stop, &count);
	CHECK_STATUS(OK);
	EXPECT_EQ(floor(stop)-ceil(start)+1, count);
	if (s_.ok() && count == floor(stop)-ceil(start)+1) {
		log_success("key�ǿգ�scoreMin<start<end<scoreMax");
	} else {
		log_fail("key�ǿգ�scoreMin<start<end<scoreMax");
	}

	write_zset_up_score(key, num);
	s_.OK();//key�ǿգ�scoreMin<end<start<scoreMax
	stop = GetRandomFloat_(0, num-2);
	start = GetRandomFloat_(ceil(stop), num-1);
	count = -1;
	s_ = n_->ZRemrangebyscore(key, start, stop, &count);
	CHECK_STATUS(OK);
	EXPECT_EQ(0, count);
	if (s_.ok() && count == 0) {
		log_success("key�ǿգ�scoreMin<end<start<scoreMax");
	} else {
		log_fail("key�ǿգ�scoreMin<end<start<scoreMax");
	}

	write_zset_up_score(key, num);
	s_.OK();//key�ǿգ�start<scoreMin��end��scores��Χ��
	start = -1;
	stop = GetRandomFloat_(0, num-1);
	count = -1;
	s_ = n_->ZRemrangebyscore(key, start, stop, &count);
	CHECK_STATUS(OK);
	EXPECT_EQ(floor(stop)+1, count);
	if (s_.ok() && count == floor(stop)+1) {
		log_success("key�ǿգ�start<scoreMin��end��scores��Χ��");
	} else {
		log_fail("key�ǿգ�start<scoreMin��end��scores��Χ��");
	}

	write_zset_up_score(key, num);
	s_.OK();//key�ǿգ�start��scores��Χ�ڣ�endΪ>scoreMax,
	start = GetRandomFloat_(0, num-2);
	stop = num+14;
	count = -1;
	s_ = n_->ZRemrangebyscore(key, start, stop, &count);
	CHECK_STATUS(OK);
	EXPECT_EQ(num-ceil(start), count);
	if (s_.ok() && count == num-ceil(start)) {
		log_success("key�ǿգ�start��scores��Χ�ڣ�endΪ>scoreMax,");
	} else {
		log_fail("key�ǿգ�start��scores��Χ�ڣ�endΪ>scoreMax,");
	}

	write_zset_up_score(key, num);
	s_.OK();//key�ǿգ�start,end��>scoreMax,��start>end
	start = num+5;
	stop = num+3;
	count = -1;
	s_ = n_->ZRemrangebyscore(key, start, stop, &count);
	CHECK_STATUS(OK);
	EXPECT_EQ(0, count);
	if (s_.ok() && count == 0) {
		log_success("key�ǿգ�start,end��>scoreMax,��start>end");
	} else {
		log_fail("key�ǿգ�start,end��>scoreMax,��start>end");
	}

	write_zset_up_score(key, num);
	s_.OK();//key�ǿգ�start,end��>scoreMax��start<end
	start = num+7;
	stop = num+10;
	count = -1;
	s_ = n_->ZRemrangebyscore(key, start, stop, &count);
	CHECK_STATUS(OK);
	EXPECT_EQ(0, count);
	if (s_.ok() && count == 0) {
		log_success("key�ǿգ�start,end��>scoreMax��start<end");
	} else {
		log_fail("key�ǿգ�start,end��>scoreMax��start<end");
	}

	write_zset_up_score(key, num);
	s_.OK();//key�ǿգ�start,end��<scoreMin��start<end
	start = -8;
	stop = -2;
	count = -1;
	s_ = n_->ZRemrangebyscore(key, start, stop, &count);
	CHECK_STATUS(OK);
	EXPECT_EQ(0, count);
	if (s_.ok() && count == 0) {
		log_success("key�ǿգ�start,end��<scoreMin��start<end");
	} else {
		log_fail("key�ǿգ�start,end��<scoreMin��start<end");
	}
	
	write_zset_up_score(key, num);
	s_.OK();//key�ǿգ�start,end��<scoreMin��start>end
	start = -3;
	stop = -23;
	count = -1;
	s_ = n_->ZRemrangebyscore(key, start, stop, &count);
	CHECK_STATUS(OK);
	EXPECT_EQ(0, count);
	if (s_.ok() && count == 0) {
		log_success("key�ǿգ�start,end��<scoreMin��start>end");
	} else {
		log_fail("key�ǿգ�start,end��<scoreMin��start>end");
	}

	write_zset_up_score(key, num);
	s_.OK();//key�ǿգ�start<scoreMin<scoreMax<end
	start = -7;
	stop = num+10;
	count = -1;
	s_ = n_->ZRemrangebyscore(key, start, stop, &count);
	CHECK_STATUS(OK);
	EXPECT_EQ(num, count);
	if (s_.ok() && count == num) {
		log_success("key�ǿգ�start<scoreMin<scoreMax<end");
	} else {
		log_fail("key�ǿգ�start<scoreMin<scoreMax<end");
	}

	write_zset_up_score(key, num);
	s_.OK();//key�ǿգ�end<scoreMin<scoreMax<start
	start = num+13;
	stop = -10;
	count = -1;
	s_ = n_->ZRemrangebyscore(key, start, stop, &count);
	CHECK_STATUS(OK);
	EXPECT_EQ(0, count);
	if (s_.ok() && count == 0) {
		log_success("key�ǿգ�end<scoreMin<scoreMax<start");
	} else {
		log_fail("key�ǿգ�end<scoreMin<scoreMax<start");
	}
	log_message("============================ZSETTEST END===========================");
	log_message("============================ZSETTEST END===========================");
}
