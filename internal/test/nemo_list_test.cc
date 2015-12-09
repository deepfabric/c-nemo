#include <string>
#include <vector>
#include <sys/time.h>
#include <cstdlib>
#include <cstdlib>
#include <string>

#include "gtest/gtest.h"
#include "xdebug.h"
#include "nemo.h"

//#include "stdint.h"
#include "nemo_list_test.h"
using namespace std;

TEST_F(NemoListTest, TestLPush)
{
	log_message("============================LISTTEST START===========================");
	log_message("============================LISTTEST START===========================");
	log_message("========TestLPush========");
	string key, val;
	int64_t llen;
	
	s_.OK();//ԭ����key���ڣ���Ĳ���list�ṹ����
	key = GetRandomKey_();
	val = GetRandomVal_();
	n_->Set(key, val);
	val = GetRandomVal_();
	llen = 0;
	s_ = n_->LPush(key, val, &llen);
	CHECK_STATUS(OK);
	EXPECT_EQ(1, llen);
	if (s_.ok() && 1 == llen) {
		log_success("ԭ����key���ڣ���Ĳ���list�ṹ����");
	} else {
		log_fail("ԭ����key���ڣ���Ĳ���list�ṹ����");
	}
	n_->LPop(key, &val);

	s_.OK();//ԭ����key�����ڣ�
	key = GetRandomKey_();
	val = GetRandomVal_();
	llen = 0;
	s_ = n_->LPush(key, val, &llen);
	CHECK_STATUS(OK);
	EXPECT_EQ(1, llen);
	if (s_.ok() && 1 == llen) {
		log_success("ԭ����key������");
	} else {
		log_fail("ԭ����key������");
	}

	s_.OK();//ԭ����key���ڣ������list�ṹ����
	val = GetRandomVal_();
	llen = 1;
	s_ = n_->LPush(key, val, &llen);
	CHECK_STATUS(OK);
	EXPECT_EQ(2, llen);
	if (s_.ok() && 2 == llen) {
		log_success("ԭ����key���ڣ������list�ṹ����");
	} else {
		log_fail("ԭ����key���ڣ������list�ṹ����");
	}

	s_.OK();//valȡ��󳤶�
	val = GetRandomBytes_(maxValLen_);
	llen = 2;
	s_ = n_->LPush(key, val, &llen);
	CHECK_STATUS(OK);
	EXPECT_EQ(3, llen);
	if (s_.ok() && 3 == llen) {
		log_success("alȡ��󳤶�");
	} else {
		log_fail("alȡ��󳤶�");
	}

	s_.OK();//valȡ��ֵ
	val = "";
	llen = 3;
	s_ = n_->LPush(key, val, &llen);
	CHECK_STATUS(OK);
	EXPECT_EQ(4, llen);
	if (s_.ok() && 4 == llen) {
		log_success("valȡ��ֵ");
	} else {
		log_fail("valȡ��ֵ");
	}
}

TEST_F(NemoListTest, TestLIndex)
{
	log_message("\n========TestLIndex========");
	string key, val, valResult;
	int64_t index;
	
#define LIndexLoopProcess(index, state, valResult, message)\
	s_ = n_->LIndex(key, index, &val);\
	CHECK_STATUS(state);\
	EXPECT_EQ(valResult, val);\
	if (string(#state) == "OK") {\
		if (s_.ok() && valResult == val) {\
			log_success(message);\
		} else {\
			log_fail(message);\
		}\
	}\
	if (string(#state) == "Corruption") {\
		if (s_.IsCorruption() && valResult == val) {\
			log_success(message);\
		} else {\
			log_fail(message);\
		}\
	}\
	if (string(#state) == "NotFound") {\
		if (s_.IsNotFound() && valResult == val) {\
			log_success(message);\
		} else {\
			log_fail(message);\
		}\
	}\
	do {} while(false)


	s_.OK();//key������
	key = GetRandomKey_();
	index = 0;
	valResult = "";
	val = "";
	LIndexLoopProcess(index, NotFound, valResult, "key������");

	s_.OK();//key���ڣ���Ĳ���list�ṹ����
	key = GetRandomKey_();
	val = GetRandomVal_();
	n_->Set(key, val);
	index = 0;
	valResult = "";
	val = "";
	LIndexLoopProcess(index, NotFound, valResult, "key���ڣ���Ĳ���list�ṹ����");

	key = "LIndex_Test";
	int64_t len = 50, tempLen;
	n_->LLen(key, &tempLen);
	while(tempLen > 0)
	{
		n_->LPop(key, &val);
		tempLen--;
	}
	for(int64_t idx = 0; idx != len; idx++)
	{
		n_->RPush(key, string("LIndex_Test_") + itoa(idx), &tempLen);
	}
	log_message("===List from left to right is LIndex_Test_0 to LIndex_Test_49");
	
	s_.OK();//key���ڣ������list ���ݣ�indexΪ������list��Χ��
	index = GetRandomUint_(0, len-1);
	LIndexLoopProcess(index, OK, string("LIndex_Test_") + itoa(index), "key���ڣ������list ���ݣ�indexΪ������list��Χ��");

	s_.OK();//key���ڣ������list ���ݣ�indexΪ����������list��Χ
	index = len;
	valResult = "";
	val = "";
	LIndexLoopProcess(index, Corruption, valResult, "key���ڣ������list ���ݣ�indexΪ����������list��Χ");

	s_.OK();//key���ڣ������list ���ݣ�indexΪ��������list��Χ
	index = GetRandomUint_(1, len);
	index = (-1)*index;
	LIndexLoopProcess(index, OK, string("LIndex_Test_") + itoa(len+index), "key���ڣ������list ���ݣ�indexΪ��������list��Χ");

	s_.OK();//key���ڣ������list ���ݣ�indexΪ����������list��Χ
	index = (len+10)*(-1);
	valResult = "";
	val = "";
	LIndexLoopProcess(index, Corruption, valResult, "key���ڣ������list ���ݣ�indexΪ����������list��Χ");
}

TEST_F(NemoListTest, TestLLen)
{
	log_message("\n========TestLLen========");
	string key, val;
	int64_t llen;

	s_.OK();//key������
	key = GetRandomKey_();
	s_ = n_->LLen(key, &llen);
	CHECK_STATUS(NotFound);
	EXPECT_EQ(0, llen);
	if (s_.IsNotFound() && 0 == llen) {
		log_success("key������");
	} else {
		log_fail("key������");
	}

	s_.OK();//key���ڣ�����Ĳ���list�ṹ����
	key = GetRandomKey_();
	val = GetRandomVal_();
	n_->Set(key, val);
	s_ = n_->LLen(key, &llen);
	EXPECT_EQ(0, llen);
	if (s_.IsNotFound() && 0 == llen) {
		log_success("key������");
	} else {
		log_fail("key������");
	}

	
	s_.OK();//Key���ڣ��Ҵ���list���ݽṹ
	key = "LLen_Test";
	int64_t len = 50, tempLen;
	n_->LLen(key, &tempLen);
	while(tempLen > 0)
	{
		n_->LPop(key, &val);
		tempLen--;
	}
	for(int64_t idx = 0; idx != len; idx++)
	{
		n_->RPush(key, string("LIndex_Test_") + itoa(idx), &tempLen);
	}
	s_ = n_->LLen(key, &llen);
	CHECK_STATUS(OK);	
	EXPECT_EQ(len, llen);
	if (s_.ok() && llen == len) {
		log_success("Key���ڣ��Ҵ���list���ݽṹ");
	} else {
		log_fail("Key���ڣ��Ҵ���list���ݽṹ");
	}
}

TEST_F(NemoListTest, TestLPop)
{
	log_message("\n========TestLPop========");
#define LPopLoopProcess(state, valResult, message)\
	s_ = n_->LPop(key, &val);\
	CHECK_STATUS(state);\
	EXPECT_EQ(valResult, val);\
	if (string(#state) == "OK") {\
		if (s_.ok() && valResult == val) {\
			log_success(message);\
		} else {\
			log_fail(message);\
		}\
	}\
	if (string(#state) == "Corruption") {\
		if (s_.IsCorruption() && valResult == val) {\
			log_success(message);\
		} else {\
			log_fail(message);\
		}\
	}\
	if (string(#state) == "NotFound") {\
		if (s_.IsNotFound() && valResult == val) {\
			log_success(message);\
		} else {\
			log_fail(message);\
		}\
	}\
	do {} while(false)

	string key, val, valResult;
	int64_t len, tempLen;
	bool flag;

	s_.OK();//ԭ����key������
	key = GetRandomKey_();
	val = "";
	valResult = "";
	LPopLoopProcess(NotFound, valResult, "ԭ����key������");

	s_.OK();//ԭ����key��Ĳ���list�ṹ
	key = GetRandomKey_();
	val = GetRandomVal_();
	n_->Set(key, val);
	val = "";
	valResult = "";
	LPopLoopProcess(NotFound, valResult, "ԭ����key��Ĳ���list�ṹ");

	s_.OK();//ԭ����key����list�����ݽṹ
	key = GetRandomKey_();
	val = GetRandomVal_();
	n_->LPush(key, val, &len);
	valResult = val;
	LPopLoopProcess(OK, valResult, "ԭ����key����list�����ݽṹ");

	s_.OK();//�������LPush���ݣ��۲�LPop�Ƿ������³�
	key = "LPop_Test";
	len = 50;
	n_->LLen(key, &tempLen);
	while(tempLen > 0)
	{
		n_->LPop(key, &val);
		tempLen--;
	}
	for(int64_t idx = 0; idx != len; idx++)
	{
		n_->RPush(key, string("LPop_Test_") + itoa(idx), &tempLen);
	}
	flag = true;
	for(int64_t idx = 0; idx != len; idx++)
	{
		s_ = n_->LPop(key, &val);
		CHECK_STATUS(OK);
		EXPECT_EQ(string("LPop_Test_")+itoa(idx), val);
		if(!s_.ok() || val != string("LPop_Test_")+itoa(idx))
			flag = false;
	}
	if (flag) {
		log_success("�������Push���ݣ��۲�LPop�Ƿ������³�");
	} else {
		log_fail("�������Push���ݣ��۲�LPop�Ƿ������³�");
	}
}

TEST_F(NemoListTest, TestLPushx)
{
#define LPushxLoopProcess(state, llenResult, message)\
	s_ = n_->LPushx(key, val, &llen);\
	CHECK_STATUS(state);\
	EXPECT_EQ(llenResult, llen);\
	if (string(#state) == "OK") {\
		if (s_.ok() && llenResult == llen) {\
			log_success(message);\
		} else {\
			log_fail(message);\
		}\
	}\
	if (string(#state) == "Corruption") {\
		if (s_.IsCorruption() && llenResult == llen) {\
			log_success(message);\
		} else {\
			log_fail(message);\
		}\
	}\
	if (string(#state) == "NotFound") {\
		if (s_.IsNotFound() && llenResult == llen) {\
			log_success(message);\
		} else {\
			log_fail(message);\
		}\
	}\
	do {} while(false)

	log_message("\n========TestLPushx========");
	string key, val;
	int64_t llen, tempLlen;

	s_.OK();//ԭ����key������
	key = GetRandomKey_();
	val = GetRandomVal_();
	LPushxLoopProcess(NotFound, 0, "ԭ����key������");

	s_.OK();//key���ڣ���������ݽṹ������û��list����
	key = GetRandomKey_();
	val = GetRandomVal_();
	n_->Set(key, val);
	val = GetRandomVal_();
	LPushxLoopProcess(NotFound, 0, "key���ڣ���������ݽṹ������û��list����");

	s_.OK();//key���ڣ���ԭ�������list���ݽṹ
	key = GetRandomKey_();
	val = GetRandomVal_();
	n_->LPush(key, val, &llen);
	val = GetRandomVal_();
	tempLlen = llen+1;
	LPushxLoopProcess(OK, tempLlen, "key���ڣ���ԭ�������list���ݽṹ");
}

TEST_F(NemoListTest, TestLRange)
{
	log_message("\n========TestLRange========");

	string key, val, tempVal;
	int64_t llen, tempLLen, begin, end;
	vector<nemo::IV> ivs;

	s_.OK();//Key������
	key = GetRandomKey_();
	n_->LLen(key, &tempLLen);
	s_ = n_->LRange(key, 0, 1, ivs);
	CHECK_STATUS(NotFound);
	EXPECT_EQ(true, ivs.empty());
	if(s_.IsNotFound() && ivs.empty())
		log_success("Key������");
	else
		log_fail("Key������");

	s_.OK();//key���ڣ���Ĳ���list�ṹ����
	key = GetRandomKey_();
	val = GetRandomVal_();
	n_->Set(key,val);
	s_ = n_->LRange(key, 0, 1, ivs);
	CHECK_STATUS(NotFound);
	EXPECT_EQ(true, ivs.empty());
	if(s_.IsNotFound() && ivs.empty())
		log_success("Key������");
	else
		log_fail("Key������");

	s_.OK();//key���ڣ���������ݽṹ������û��list����
	key = GetRandomKey_();
	val = GetRandomVal_();
	n_->LPush(key, val, &tempLLen);
	while(tempLLen > 0)
	{
		n_->LPop(key, &tempVal);
		tempLLen--;
	}
	s_ = n_->LRange(key, 0, 1, ivs);
	CHECK_STATUS(NotFound);
	EXPECT_EQ(true, ivs.empty());
	if(s_.IsNotFound() && ivs.empty())
		log_success("key���ڣ���������ݽṹ������û��list����");
	else
		log_fail("key���ڣ���������ݽṹ������û��list����");


	llen = 50;
	key = "LRange_Test";
	n_->LLen(key, &tempLLen);
	while(tempLLen > 0)
	{
		n_->LPop(key, &tempVal);	
		tempLLen--;
	}
	for(int64_t idx = 0; idx != llen; idx++)
	{
		n_->RPush(key, string("LRange_Test_") + itoa(idx), &tempLLen);
	}
	log_message("===List from left to right is LRange_Test_0 to LRange_Test_%lld", llen);

	string startVal, endVal;

#define LRangeLoopProcess(ivsNum, startVal, EndVal, message)\
	ivs.clear();\
	s_ = n_->LRange(key, begin, end, ivs);\
	CHECK_STATUS(OK);\
	if (ivsNum > 0) {\
		EXPECT_EQ(ivsNum, ivs.size());\
		EXPECT_EQ(startVal, ivs.front().val);\
		EXPECT_EQ(endVal, ivs.back().val);\
		if(s_.ok() && ((ivsNum) == ivs.size()) && ((startVal) == ivs.front().val) && ((endVal) == ivs.back().val))\
			log_success(message);\
		else\
			log_fail(message);\
	} else {\
		EXPECT_EQ(0, ivs.size());\
		if(s_.ok() && 0 == ivs.size())\
			log_success(message);\
		else\
			log_fail(message);\
	}\
	do { } while(false)


	


	s_.OK();//key���ڣ���list�ṹ���ݣ�0<begin<end<length����Ч��Χ��
	begin = GetRandomUint_(0, llen-1);
	end = GetRandomUint_(begin, llen-1);
	n_->LIndex(key, begin, &startVal);
	n_->LIndex(key, end, &endVal);
	LRangeLoopProcess(end-begin+1, startVal, endVal, "key���ڣ���list�ṹ���ݣ�0<begin<end<length����Ч��Χ��");
	
	s_.OK();//key���ڣ���list�ṹ���ݣ�-length<begin<0, end����Ч��Χ��end
	begin = GetRandomUint_(0, llen-1);
	end = GetRandomUint_(begin, llen-1);
	begin = begin-llen;
	n_->LIndex(key, begin, &startVal);
	n_->LIndex(key, end, &endVal);
	LRangeLoopProcess(end-begin-llen+1, startVal, endVal, "key���ڣ���list�ṹ���ݣ�-length<begin<0, end����Ч��Χ��end");

	s_.OK();//key���ڣ���list�ṹ���ݣ�begin <-length,end ����Ч��Χ��end
	begin = (-1)*(llen+10);
	end = GetRandomUint_(0, llen-1);
	n_->LIndex(key, 0, &startVal);
	n_->LIndex(key, end, &endVal);
	LRangeLoopProcess(end+1, startVal, endVal, "key���ڣ���list�ṹ���ݣ�-length<begin<0, end����Ч��Χ��end");

	s_.OK();//key���ڣ���list�ṹ���ݣ�begin > length,end ����Ч��Χ��end
	begin = llen+10;
	end = GetRandomUint_(0, llen-1);
	startVal = "";
	endVal = "";
	LRangeLoopProcess(0, startVal, endVal, "key���ڣ���list�ṹ���ݣ�begin > length,end ����Ч��Χ��end");

	s_.OK();//key���ڣ���list�ṹ���ݣ�begin����Ч��Χ��,-length<end<0
	begin = GetRandomUint_(0, llen-1);
	end = GetRandomUint_(begin, llen-1);
	end = end-llen;
	n_->LIndex(key, begin, &startVal);
	n_->LIndex(key, end, &endVal);
	LRangeLoopProcess(llen+end-begin+1, startVal, endVal, "key���ڣ���list�ṹ���ݣ�begin����Ч��Χ��,-length<end<0");

	s_.OK();//key���ڣ���list�ṹ���ݣ�begin����Ч��Χ��,end<-length
	begin = GetRandomUint_(0, llen-1);
	end = (-1)*(llen+20);
	startVal = "";
	endVal = "";
	LRangeLoopProcess(0, startVal, endVal, "key���ڣ���list�ṹ���ݣ�begin����Ч��Χ��,end<-length");

	s_.OK();//key���ڣ���list�ṹ���ݣ�begin����Ч��Χ��,end>length
	begin = GetRandomUint_(0, llen-1);
	end = llen+20;
	n_->LIndex(key, begin, &startVal);
	n_->LIndex(key, llen-1, &endVal);
	LRangeLoopProcess(llen-begin, startVal, endVal, "key���ڣ���list�ṹ���ݣ�begin����Ч��Χ��,end>length");

	ivs.clear();
}

TEST_F(NemoListTest, TestLSet)
{
	log_message("\n========TestLSet========");
	string key, val, getVal;

	s_.OK();//Key������
	key = GetRandomKey_();
	val = GetRandomVal_();
	s_ = n_->LSet(key, 0, val);
	CHECK_STATUS(NotFound);
	if(s_.IsNotFound())
		log_success("Key������");
	else
		log_fail("Key������");

	s_.OK();//Key���ڣ�����ǲ���List�ṹ
	n_->Set(key, val);
	val = GetRandomVal_();
	s_ = n_->LSet(key, 0, val);
	CHECK_STATUS(NotFound);
	if(s_.IsNotFound())
		log_success("Key���ڣ�����ǲ���List�ṹ");
	else
		log_fail("Key���ڣ�����ǲ���List�ṹ");


	int64_t llen = 10, tempLLen, index;
	key = "LSet_Test";
	n_->LLen(key, &tempLLen);
	while(tempLLen > 0)
	{
		n_->LPop(key, &getVal);	
		tempLLen--;
	}
	for(int64_t idx = 0; idx != llen; idx++)
	{
		n_->RPush(key, string("LRange_Test_") + itoa(idx), &tempLLen);
	}
	log_message("===List from left to right is LSet_Test_0 to LSet_Test_%lld", llen);

	s_.OK();//Key���ڣ������List�ṹ��Index��list��Χ�ڣ�indexΪ��
	val = GetRandomVal_();
	index = (int64_t)(GetRandomUint_(0, llen-1));
	s_ = n_->LSet(key, index, val);
	CHECK_STATUS(OK);
	n_->LIndex(key, index, &getVal);
	EXPECT_EQ(val, getVal);
	if(s_.ok() && val == getVal)
		log_success("Key���ڣ������List�ṹ��Index��list��Χ�ڣ�indexΪ��");
	else
		log_fail("Key���ڣ������List�ṹ��Index��list��Χ�ڣ�indexΪ��");

	s_.OK();//Key���ڣ������List�ṹ��Index��list��Χ�ڣ�indexΪ��
	val = GetRandomVal_();
	index = (-1)*(int64_t)(GetRandomUint_(1, llen));
	s_ = n_->LSet(key, index, val);
	CHECK_STATUS(OK);
	n_->LIndex(key, index, &getVal);
	EXPECT_EQ(val, getVal);
	if(s_.ok() && val == getVal)
		log_success("Key���ڣ������List�ṹ��Index��list��Χ�ڣ�indexΪ��");
	else
		log_fail("Key���ڣ������List�ṹ��Index��list��Χ�ڣ�indexΪ��");

	s_.OK();//Key���ڣ������List�ṹ��Index����list��Χ��
	val = GetRandomVal_();
	index = llen+10;
	s_ = n_->LSet(key, index, val);
	CHECK_STATUS(Corruption);
	if(s_.IsCorruption())
		log_success("Key���ڣ������List�ṹ��Index����list��Χ��");
	else
		log_fail("Key���ڣ������List�ṹ��Index����list��Χ��");
}

TEST_F(NemoListTest, TestLTrim)
{
	log_message("\n========TestLTrim========");
	string key, val;
	int64_t begin, end, llen, llenResult;

	s_.OK();//Key������/Key���ڣ�����ǲ���List�ṹ/Key���ڣ������List�ṹ������listΪ��
	key = GetRandomKey_();
	s_ = n_->LTrim(key, 0, 0);
	CHECK_STATUS(NotFound);
	if(s_.IsNotFound())
		log_success("Key������/Key���ڣ�����ǲ���List�ṹ/Key���ڣ������List�ṹ������listΪ��");
	else
		log_fail("Key������/Key���ڣ�����ǲ���List�ṹ/Key���ڣ������List�ṹ������listΪ��");
	
	llen = 10;
	key = "HTrim_Test";

	s_.OK();//key���ڣ���list�ṹ���ݣ�0<begin<end<length����Ч��Χ��
	write_list(llen, key);
	begin = GetRandomUint_(0, llen-1);
	end = GetRandomUint_(begin, llen-1);
	s_ = n_->LTrim(key, begin, end);
	CHECK_STATUS(OK);
	n_->LLen(key, &llenResult);
	EXPECT_EQ(end-begin+1, llenResult);
	if(s_.ok() && llenResult == (end-begin+1))
		log_success("key���ڣ���list�ṹ���ݣ�0<begin<end<length����Ч��Χ��");
	else
		log_fail("key���ڣ���list�ṹ���ݣ�0<begin<end<length����Ч��Χ��");

	s_.OK();//key���ڣ���list�ṹ���ݣ�-length<begin<0, end����Ч��Χ��end
	write_list(llen, key);
	begin = GetRandomUint_(0, llen-1);
	end = GetRandomUint_(begin, llen-1);
	begin = begin-llen;
	s_ = n_->LTrim(key, begin, end);
	CHECK_STATUS(OK);
	n_->LLen(key, &llenResult);
	EXPECT_EQ(end-begin-llen+1, llenResult);
	if(s_.ok() && (end-begin-llen+1)==llenResult)
		log_success("key���ڣ���list�ṹ���ݣ�-length<begin<0, end����Ч��Χ��end");
	else
		 log_fail("key���ڣ���list�ṹ���ݣ�-length<begin<0, end����Ч��Χ��end");

	s_.OK();//key���ڣ���list�ṹ���ݣ�begin <-length,end ����Ч��Χ��end
	write_list(llen, key);
	begin = (-1)*(llen+1);
	end = GetRandomUint_(0, llen-1);
	s_ = n_->LTrim(key, begin, end);
	CHECK_STATUS(OK);
	n_->LLen(key, &llenResult);
	EXPECT_EQ(end+1, llenResult);
	if(s_.ok() && (end+1)==llenResult)
		log_success("key���ڣ���list�ṹ���ݣ�begin <-length,end ����Ч��Χ��end");
	else
		log_fail("key���ڣ���list�ṹ���ݣ�begin <-length,end ����Ч��Χ��end");

	s_.OK();//key���ڣ���list�ṹ���ݣ�begin > length,end ����Ч��Χ��end
	write_list(llen, key);
	begin = llen;
	end = GetRandomUint_(0, llen-1);
	s_ = n_->LTrim(key, begin, end);
	CHECK_STATUS(OK);
	n_->LLen(key, &llenResult);
	EXPECT_EQ(0, llenResult);
	if(s_.ok() && 0 == llenResult)
		log_success("key���ڣ���list�ṹ���ݣ�begin > length,end ����Ч��Χ��end");
	else
		log_fail("key���ڣ���list�ṹ���ݣ�begin > length,end ����Ч��Χ��end");

	s_.ok();//key���ڣ���list�ṹ���ݣ�begin����Ч��Χ��,-length<end<0
	begin = GetRandomUint_(0, llen-1);
	end = GetRandomUint_(begin, llen-1);
	end = end-llen;
	write_list(llen, key);
	s_ = n_->LTrim(key, begin, end);
	CHECK_STATUS(OK);
	n_->LLen(key, &llenResult);
	EXPECT_EQ(end+llen-begin+1, llenResult);
	if(s_.ok() && (end+llen-begin+1)==llenResult)
		log_success("key���ڣ���list�ṹ���ݣ�begin����Ч��Χ��,-length<end<0");
	else
		log_fail("key���ڣ���list�ṹ���ݣ�begin����Ч��Χ��,-length<end<0");

	s_.OK();//key���ڣ���list�ṹ���ݣ�begin����Ч��Χ��,end<-length
	begin = GetRandomUint_(0, llen-1);
	end = (-1)*(llen+1);
	write_list(llen, key);
	s_ = n_->LTrim(key, begin, end);
	CHECK_STATUS(OK);
	n_->LLen(key, &llenResult);	
	EXPECT_EQ(0, llenResult);
	if(s_.ok() && 0==llenResult)
		log_success("key���ڣ���list�ṹ���ݣ�begin����Ч��Χ��,end<-length");
	else
		log_fail("key���ڣ���list�ṹ���ݣ�begin����Ч��Χ��,end<-length");
	
	s_.OK();//key���ڣ���list�ṹ���ݣ�begin����Ч��Χ��,end>length
	begin = GetRandomUint_(0, llen-1);
	end = llen;
	write_list(llen, key);
	s_ = n_->LTrim(key, begin, end);
	CHECK_STATUS(OK);
	n_->LLen(key, &llenResult);
	EXPECT_EQ(llen-begin, llenResult);
	if(s_.ok() && (llen-begin)==llenResult)
		log_success("key���ڣ���list�ṹ���ݣ�begin����Ч��Χ��,end<-length");
	else
		log_fail("key���ڣ���list�ṹ���ݣ�begin����Ч��Χ��,end<-length");

	s_.ok();//key���ڣ���list�ṹ���ݣ�0<=end<begin<length
	begin = GetRandomUint_(1, llen-1);
	end = GetRandomUint_(0, begin-1);
	write_list(llen, key);
	s_ = n_->LTrim(key, begin, end);
	CHECK_STATUS(OK);
	n_->LLen(key, &llenResult);
	EXPECT_EQ(0, llenResult);
	if(s_.ok() && 0 == llenResult)
		log_success("key���ڣ���list�ṹ���ݣ�0<=end<begin<length");
	else
		log_fail("key���ڣ���list�ṹ���ݣ�0<=end<begin<length");
}

TEST_F(NemoListTest, TestRPush)
{
	log_message("\n========TestRPush========");
	string key, val;
	int64_t llen;
	
	s_.OK();//ԭ����key���ڣ���Ĳ���list�ṹ����
	key = GetRandomKey_();
	val = GetRandomVal_();
	n_->Set(key, val);
	val = GetRandomVal_();
	llen = 0;
	s_ = n_->RPush(key, val, &llen);
	CHECK_STATUS(OK);
	EXPECT_EQ(1, llen);
	if(s_.ok() && 1 == llen)
		log_success("ԭ����key���ڣ���Ĳ���list�ṹ����");
	else
		log_fail("ԭ����key���ڣ���Ĳ���list�ṹ����");
	n_->LPop(key, &val);

	s_.OK();//ԭ����key�����ڣ�
	key = GetRandomKey_();
	val = GetRandomVal_();
	llen = 0;
	s_ = n_->RPush(key, val, &llen);
	CHECK_STATUS(OK);
	EXPECT_EQ(1, llen);
	if(s_.ok() && 1 == llen)
		log_success("ԭ����key������");
	else
		log_fail("ԭ����key������");

	s_.OK();//ԭ����key���ڣ������list�ṹ����
	val = GetRandomVal_();
	llen = 1;
	s_ = n_->RPush(key, val, &llen);
	CHECK_STATUS(OK);
	EXPECT_EQ(2, llen);
	if(s_.ok() && 2 == llen)
		log_success("ԭ����key���ڣ������list�ṹ����");
	else
		log_fail("ԭ����key���ڣ������list�ṹ����");

	s_.OK();//valȡ��󳤶�
	val = GetRandomBytes_(maxValLen_);
	llen = 2;
	s_ = n_->RPush(key, val, &llen);
	CHECK_STATUS(OK);
	EXPECT_EQ(3, llen);
	if(s_.ok() && 3 == llen)
		log_success("alȡ��󳤶�");
	else
		log_fail("alȡ��󳤶�");

	s_.OK();//valȡ��ֵ
	val = "";
	llen = 3;
	s_ = n_->RPush(key, val, &llen);
	CHECK_STATUS(OK);
	EXPECT_EQ(4, llen);
	if(s_.ok() && 4 == llen)
		log_success("valȡ��ֵ");
	else
		log_fail("valȡ��ֵ");
}

TEST_F(NemoListTest, TestRPushx)
{
#define RPushxLoopProcess(state, llenResult, message)\
	s_ = n_->RPushx(key, val, &llen);\
	CHECK_STATUS(state);\
	EXPECT_EQ(llenResult, llen);\
	if(string(#state) == "OK")\
		if(s_.ok() && llenResult == llen)\
			log_success(message);\
		else\
			log_fail(message);\
	if(string(#state) == "Corruption")\
		if(s_.IsCorruption() && llenResult == llen)\
			log_success(message);\
		else\
			log_fail(message);\
	if(string(#state) == "NotFound")\
		if(s_.IsNotFound() && llenResult == llen)\
			log_success(message);\
		else\
			log_fail(message)

	log_message("\n========TestRPushx========");
	string key, val;
	int64_t llen, tempLlen;

	s_.OK();//ԭ����key������
	key = GetRandomKey_();
	val = GetRandomVal_();
	RPushxLoopProcess(NotFound, 0, "ԭ����key������");

	s_.OK();//key���ڣ���������ݽṹ������û��list����
	key = GetRandomKey_();
	val = GetRandomVal_();
	n_->Set(key, val);
	val = GetRandomVal_();
	RPushxLoopProcess(NotFound, 0, "key���ڣ���������ݽṹ������û��list����");

	s_.OK();//key���ڣ���ԭ�������list���ݽṹ
	key = GetRandomKey_();
	val = GetRandomVal_();
	n_->LPush(key, val, &llen);
	val = GetRandomVal_();
	tempLlen = llen+1;
	RPushxLoopProcess(OK, tempLlen, "key���ڣ���ԭ�������list���ݽṹ");
}

TEST_F(NemoListTest, TestRPop)
{
	log_message("\n========TestRPop========");
#define RPopLoopProcess(state, valResult, message)\
	s_ = n_->RPop(key, &val);\
	CHECK_STATUS(state);\
	EXPECT_EQ(valResult, val);\
	if(string(#state) == "OK")\
		if(s_.ok() && valResult == val)\
			log_success(message);\
		else\
			log_fail(message);\
	if(string(#state) == "Corruption")\
		if(s_.IsCorruption() && valResult == val)\
			log_success(message);\
		else\
			log_fail(message);\
	if(string(#state) == "NotFound")\
		if(s_.IsNotFound() && valResult == val)\
			log_success(message);\
		else\
			log_fail(message)

	string key, val, valResult;
	int64_t len, tempLen;
	bool flag;

	s_.OK();//ԭ����key������
	key = GetRandomKey_();
	val = "";
	valResult = "";
	RPopLoopProcess(NotFound, valResult, "ԭ����key������");

	s_.OK();//ԭ����key��Ĳ���list�ṹ
	key = GetRandomKey_();
	val = GetRandomVal_();
	n_->Set(key, val);
	val = "";
	valResult = "";
	RPopLoopProcess(NotFound, valResult, "ԭ����key��Ĳ���list�ṹ");

	s_.OK();//ԭ����key����list�����ݽṹ
	key = GetRandomKey_();
	val = GetRandomVal_();
	n_->RPush(key, val, &len);
	valResult = val;
	RPopLoopProcess(OK, valResult, "ԭ����key����list�����ݽṹ");

	s_.OK();//�������RPush���ݣ��۲�RPop�Ƿ������³�
	key = "RPop_Test";
	len = 50;
	n_->LLen(key, &tempLen);
	while(tempLen > 0)
	{
		n_->RPop(key, &val);
		tempLen--;
	}
	for(int64_t idx = 0; idx != len; idx++)
	{
		n_->LPush(key, string("RPop_Test_") + itoa(idx), &tempLen);
	}
	flag = true;
	for(int64_t idx = 0; idx != len; idx++)
	{
		s_ = n_->RPop(key, &val);
		CHECK_STATUS(OK);
		EXPECT_EQ(string("RPop_Test_")+itoa(idx), val);
		if(!s_.ok() || val != string("RPop_Test_")+itoa(idx))
			flag = false;
	}
	if(flag)
		log_success("�������Push���ݣ��۲�RPop�Ƿ������³�");
	else
		log_fail("�������Push���ݣ��۲�RPop�Ƿ������³�");
}

TEST_F(NemoListTest, TestRPopLPush)
{
	log_message("\n========TestRPopLPush========");
	string keySrc, keyDst, valSrc, valDst, val;
	int64_t llen;

	
	s_.OK();//srcKey�����ڣ�
	keySrc = GetRandomKey_();
	keyDst = GetRandomKey_();
	s_ = n_->RPopLPush(keySrc, keyDst, val);
	CHECK_STATUS(NotFound);
	if(s_.IsNotFound())
		log_success("srcKey������");
	else
		log_fail("srcKey������");
	
	s_.OK();//srcKey���ڣ���list���ݽṹ��dstKey������
	valSrc = GetRandomVal_();
	n_->LPush(keySrc, valSrc, &llen);
	s_ = n_->RPopLPush(keySrc, keyDst, val);
	CHECK_STATUS(OK);
	n_->LIndex(keyDst, 0, &valDst);
	EXPECT_EQ(valSrc, valDst);
	if(s_.ok() && valSrc == valDst)
		log_success("srcKey���ڣ���list���ݽṹ��dstKey������");
	else
		log_fail("srcKey���ڣ���list���ݽṹ��dstKey������");

	s_.OK();//srcKey���ڣ���list���ݽṹ��dstKey���ڣ�����list ���ݽṹ
	valSrc = GetRandomVal_();
	n_->RPush(keySrc, valSrc, &llen);
	s_ = n_->RPopLPush(keySrc, keyDst, val);
	CHECK_STATUS(OK);
	n_->LIndex(keyDst, 0, &valDst);
	EXPECT_EQ(valSrc, valDst);
	if(s_.ok() && valSrc==valDst)
		log_success("srcKey���ڣ���list���ݽṹ��dstKey���ڣ�����list ���ݽṹ");
	else
		log_fail("srcKey���ڣ���list���ݽṹ��dstKey���ڣ�����list ���ݽṹ");
}

TEST_F(NemoListTest, TestLInsert)
{
	log_message("\n========TestLInsert========");
	string key, val, pivot;
	nemo::Position pos;
	int64_t llen;
	
	s_.OK();//Key������/Key���ڣ�����ǲ���List�ṹ/Key���ڣ������List�ṹ������listΪ��
	pos = nemo::AFTER;
	key = GetRandomKey_();
	s_ = n_->LInsert(key, pos, pivot, val, &llen);
	CHECK_STATUS(NotFound);
	EXPECT_EQ(0, llen);
	if(s_.IsNotFound() && 0 == llen)
		log_success("Key������/Key���ڣ�����ǲ���List�ṹ/Key���ڣ������List�ṹ������listΪ��");
	else
		log_fail("Key������/Key���ڣ�����ǲ���List�ṹ/Key���ڣ������List�ṹ������listΪ��");

	s_.OK();//Key���ڣ�����list���ݽṹ��
	val = GetRandomVal_();
	llen = 10;
	key = "LInsert_Test";
	write_list(llen, key);
	pivot = key+"_"+itoa(GetRandomUint_(0, llen-1));
	s_ = n_->LInsert(key, pos, pivot, val, &llen);
	CHECK_STATUS(OK);
	EXPECT_EQ(11, llen);
	if(s_.ok() && llen == 11)
		log_success("Key���ڣ�����list���ݽṹ");
	else
		log_fail("Key���ڣ�����list���ݽṹ");

	s_.OK();//Key���ڣ�����list���ݽṹ������û��pivot
	val = GetRandomVal_();
	llen = 10;
	key = "LInsert_Test";
	write_list(llen, key);
	pivot = key+"_"+itoa(llen+10);
	s_ = n_->LInsert(key, pos, pivot, val, &llen);
	CHECK_STATUS(OK);
	EXPECT_EQ(-1, llen);
	if(s_.ok() && -1 == llen)
		log_success("Key���ڣ�����list���ݽṹ������û��pivot");
	else
		log_fail("Key���ڣ�����list���ݽṹ������û��pivot");
}

TEST_F(NemoListTest, TestLRem)
{
	log_message("\n========TestLRem========");
	string key, val;
	int64_t count, rem_count;

	s_.OK();//Key������/Key���ڣ�����ǲ���List�ṹ/Key���ڣ������List�ṹ������listΪ��
	key = GetRandomKey_();
	val = GetRandomVal_();
	rem_count = -1;
	s_ = n_->LRem(key, 0, val, &rem_count);
	CHECK_STATUS(NotFound);
	EXPECT_EQ(0, rem_count);
	if(s_.IsNotFound() && 0 == rem_count)
		log_success("Key������/Key���ڣ�����ǲ���List�ṹ/Key���ڣ������List�ṹ������listΪ��");
	else
		log_fail("Key������/Key���ڣ�����ǲ���List�ṹ/Key���ڣ������List�ṹ������listΪ��");

	s_.OK();//Key���ڣ��Ҵ����list���ݣ�count=0; ��count��< ��val�ĸ�����
	int64_t llen, tempLLen;
	string tempVal;
	key = "LRem_Test";
	n_->LLen(key, &tempLLen);
	while(tempLLen > 0)
	{
		n_->LPop(key, &tempVal);
		tempLLen--;
	}
	llen = 10;
	for(int index = 0; index != llen; index++)
	{
		n_->RPush(key, key+"_val", &tempLLen);
	}
	count = 0;
	val = key+"_val";
	s_ = n_->LRem(key, count, val, &rem_count);
	CHECK_STATUS(OK);
	EXPECT_EQ(llen, rem_count);
	if(s_.ok() && llen == rem_count)
		log_success("Key���ڣ��Ҵ����list���ݣ�count=0; ��count��< ��val�ĸ���");
	else
		log_fail("Key���ڣ��Ҵ����list���ݣ�count=0; ��count��< ��val�ĸ���");

	s_.OK();//Key���ڣ��Ҵ����list���ݣ�count<0; ��count��< ��val�ĸ�����
	key = "LRem_Test";
	n_->LLen(key, &tempLLen);
	while(tempLLen > 0)
	{
		n_->LPop(key, &tempVal);
		tempLLen--;
	}
	llen = 10;
	for(int index = 0; index != llen; index++)
	{
		n_->RPush(key, key+"_val", &tempLLen);
	}
	count = GetRandomUint_(1, llen-1);
	count = (-1)*count;
	val = key+"_val";
	s_ = n_->LRem(key, count, val, &rem_count);
	CHECK_STATUS(OK);
	EXPECT_EQ((-1)*count, rem_count);
	if(s_.ok() && rem_count == (-1)*count)
		log_success("Key���ڣ��Ҵ����list���ݣ�count<0; ��count��< ��val�ĸ�����");
	else
		log_fail("Key���ڣ��Ҵ����list���ݣ�count<0; ��count��< ��val�ĸ�����");


	s_.OK();//Key���ڣ��Ҵ����list���ݣ�count<0; ��count��> ��val�ĸ�����
	key = "LRem_Test";
	n_->LLen(key, &tempLLen);
	while(tempLLen > 0)
	{
		n_->LPop(key, &tempVal);
		tempLLen--;
	}
	llen = 10;
	for(int index = 0; index != llen; index++)
	{
		n_->RPush(key, key+"_val", &tempLLen);
	}
	count = llen+30;
	count = (-1)*count;
	val = key+"_val";
	s_ = n_->LRem(key, count, val, &rem_count);
	CHECK_STATUS(OK);
	EXPECT_EQ(llen, rem_count);
	if(s_.ok() && llen == rem_count)
		log_success("Key���ڣ��Ҵ����list���ݣ�count<0; ��count��> ��val�ĸ���");
	else
		log_fail("Key���ڣ��Ҵ����list���ݣ�count<0; ��count��> ��val�ĸ���");


	s_.OK();//Key���ڣ��Ҵ����list���ݣ�count>0; ��count��< ��val�ĸ�����
	key = "LRem_Test";
	n_->LLen(key, &tempLLen);
	while(tempLLen > 0)
	{
		n_->LPop(key, &tempVal);
		tempLLen--;
	}
	llen = 10;
	for(int index = 0; index != llen; index++)
	{
		n_->RPush(key, key+"_val", &tempLLen);
	}
	count = GetRandomUint_(1, llen-1);
	val = key+"_val";
	s_ = n_->LRem(key, count, val, &rem_count);
	CHECK_STATUS(OK);
	EXPECT_EQ(count, rem_count);
	if(s_.ok() && count == rem_count)
		log_success("Key���ڣ��Ҵ����list���ݣ�count>0; ��count��< ��val�ĸ�����");
	else
		log_fail("Key���ڣ��Ҵ����list���ݣ�count>0; ��count��< ��val�ĸ�����");

	s_.OK();//Key���ڣ��Ҵ����list���ݣ�count>0; ��count��> ��val�ĸ�����
	key = "LRem_Test";
	n_->LLen(key, &tempLLen);
	while(tempLLen > 0)
	{
		n_->LPop(key, &tempVal);
		tempLLen--;
	}
	llen = 10;
	for(int index = 0; index != llen; index++)
	{
		n_->RPush(key, key+"_val", &tempLLen);
	}
	count = llen+20;
	val = key+"_val";
	s_ = n_->LRem(key, count, val, &rem_count);
	CHECK_STATUS(OK);
	EXPECT_EQ(llen, rem_count);
	if(s_.ok() && llen == rem_count)
		log_success("Key���ڣ��Ҵ����list���ݣ�count>0; ��count��> ��val�ĸ�����");
	else
		log_fail("Key���ڣ��Ҵ����list���ݣ�count>0; ��count��> ��val�ĸ�����");
	log_message("============================LISTTEST END===========================");
	log_message("============================LISTTEST END===========================\n\n");
}
