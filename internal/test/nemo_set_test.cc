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
#include "nemo_set_test.h"
using namespace std;


TEST_F(NemoSetTest, TestSAdd) {
	log_message("============================SETTEST START===========================");
	log_message("============================SETTEST START===========================");
	log_message("========TestSAdd========");
	string key, member, getMember;
	int64_t res;
	
	s_.OK();//ԭ����key������
	key = GetRandomKey_();
	deleteAllMembers(key);
	member = GetRandomVal_();
	s_ = n_->SAdd(key, member, &res);
	CHECK_STATUS(OK);
	EXPECT_EQ(1, res);
	getMember = "";
	n_->SPop(key, getMember);
	EXPECT_EQ(member, getMember);
	if (s_.ok() && 1 == res && member == getMember) {
		log_success("ԭ����key������");
	} else {
		log_fail("ԭ����key������");
	}
	
	s_.OK();//ԭ����key���ڣ���Ĳ���Set�ṹ����
	key = GetRandomKey_();
	deleteAllMembers(key);
	member = GetRandomVal_();
	n_->Set(key, member);
	s_ = n_->SAdd(key, member, &res);
	CHECK_STATUS(OK);
	EXPECT_EQ(1, res);
	getMember = "";
	n_->SPop(key, getMember);
	EXPECT_EQ(member, getMember);	
	if (s_.ok() && 1 == res && member == getMember) {
		log_success("ԭ����key���ڣ���Ĳ���Set�ṹ����");
	} else {
		log_fail("ԭ����key���ڣ���Ĳ���Set�ṹ����");
	}

	s_.OK();//ԭ����key���ڣ������Set�����ݽṹ������member
	key = GetRandomKey_();
	member = GetRandomVal_();
	n_->SAdd(key, member, &res);
	s_ = n_->SAdd(key, member, &res);
	CHECK_STATUS(OK);
	EXPECT_EQ(0, res);
	if (s_.ok() && res == 0) {
		log_success("ԭ����key���ڣ������Set�����ݽṹ��������member�����������");
	} else {
		log_fail("ԭ����key���ڣ������Set�����ݽṹ��������member�����������");
	}

	s_.OK();//ԭ����key���ڣ������Set�����ݽṹ��������member�����������
	key = GetRandomKey_();
	member = GetRandomVal_();
	n_->SAdd(key, member, &res);
	res = 0;
	member = GetRandomVal_();
	s_ = n_->SAdd(key, member, &res);
	CHECK_STATUS(OK);
	EXPECT_EQ(1, res);
	if (s_.ok() && res == 1) {
		log_success("ԭ����key���ڣ������Set�����ݽṹ��������member�����������");
	} else {
		log_fail("ԭ����key���ڣ������Set�����ݽṹ��������member�����������");
	}

	s_.OK();//memberȡval����󳤶ȣ������������
	member = GetRandomBytes_(maxValLen_);
	deleteAllMembers(key);
	s_ = n_->SAdd(key, member, &res);
	CHECK_STATUS(OK);
	EXPECT_EQ(1, res);
	getMember = "";
	n_->SPop(key, getMember);
	EXPECT_EQ(member, getMember);
	if (s_.ok() && res == 1 && member == getMember) {
		log_success("memberȡkey�����ֵ�������������");
	} else {
		log_fail("memberȡkey�����ֵ�������������");
	}

	s_.OK();//memberȡ��ֵ�������������
	member = "";
	s_ = n_->SAdd(key, member, &res);
	CHECK_STATUS(OK);
	EXPECT_EQ(1, res);
	if (s_.ok() && res == 1) {
		log_success("memberȡ��ֵ�������������");
	} else {
		log_fail("memberȡ��ֵ�������������");
	}

	s_.OK();//keyȡ��ֵ
	key = "";
	member = GetRandomVal_();
	s_ = n_->SAdd(key, member, &res);
	CHECK_STATUS(OK);
	EXPECT_EQ(1, res);
	if (s_.ok() && 1 == res) {
		log_success("keyȡ��ֵ");
	} else {
		log_fail("keyȡ��ֵ");
	}
	n_->SRem(key, member, &res);
}

TEST_F(NemoSetTest, TestSRem) {
	log_message("\n========TestSRem========");
	string key, member;
	int64_t res;

#define SRemLoopProcess(expectedState, expectedRes, testMessage)\
	s_ = n_->SRem(key, member, &res);\
	CHECK_STATUS(expectedState);\
	EXPECT_EQ(expectedRes, res);\
	if (string(#expectedState) == "OK") {\
		if (s_.ok() && expectedRes == res) {\
			log_success(testMessage);\
		} else {\
			log_fail(testMessage);\
		}\
	} else if(string(#expectedState) == "NotFound") {\
		if(s_.IsNotFound() && expectedRes == res) {\
			log_success(testMessage);\
		} else {\
			log_fail(testMessage);\
		}\
	}\
	do{} while(false)
	

	s_.OK();//ԭ����key������
	key = GetRandomKey_();
	member = GetRandomVal_();
	SRemLoopProcess(NotFound, 0, "ԭ����key������");
	
	s_.OK();//ԭ����key���ڣ���Ĳ���Set�ṹ����
	key = GetRandomKey_();
	member = GetRandomVal_();
	n_->Set(key, member);
	SRemLoopProcess(NotFound, 0, "ԭ����key���ڣ���Ĳ���Set�ṹ����");

	s_.OK();//ԭ����key���ڣ������Set�����ݽṹ��������member
	key = GetRandomKey_();
	member = GetRandomVal_();
	n_->SAdd(key, member, &res);
	member = GetRandomVal_();
	res = -1;
	SRemLoopProcess(NotFound, 0, "ԭ����key���ڣ������Set�����ݽṹ��������member");

	s_.OK();//ԭ����key���ڣ������Set�����ݽṹ������member
	key = GetRandomKey_();
	member = GetRandomVal_();
	n_->SAdd(key, member, &res);
	res = -1;
	SRemLoopProcess(OK, 1, "ԭ����key���ڣ������Set�����ݽṹ������member");
}

TEST_F(NemoSetTest, TestSCard) {
	log_message("\n========TestSRem========");

	string key, member;
	int64_t res, card;

	s_.OK();//ԭ����key���ڣ�ԭ����key���ڣ���Ĳ���Set�ṹ����
	key = GetRandomKey_();
	card = n_->SCard(key);
	EXPECT_EQ(0, card);
	if (0 == card) {
		log_success("ԭ����key���ڣ�ԭ����key���ڣ���Ĳ���Set�ṹ����");
	} else {
		log_fail("ԭ����key���ڣ�ԭ����key���ڣ���Ĳ���Set�ṹ����");
	}

	s_.OK();//ԭ����key���ڣ������Set�����ݽṹ
	int64_t num = 13;
	key = "SCard_Test";
	for(int index = 0; index != num; index++) {
		n_->SAdd(key, key+"_"+itoa(index), &res);
	}
	card = n_->SCard(key);
	EXPECT_EQ(num, card);
	if (num == card) {
		log_success("ԭ����key���ڣ������Set�����ݽṹ");
	} else {
		log_fail("ԭ����key���ڣ������Set�����ݽṹ");
	}
}

TEST_F(NemoSetTest, TestSScan) {
	log_message("\n========TestSScan========");

	string key, member;
	nemo::SIterator* siter;
	int64_t num, limit;
	
#define SScanLoopProcess(limit, expectedNum, testMessage)\
	siter = n_->SScan(key, limit);\
	num = 0;\
	while (siter->Next()) {\
		num++;\
	}\
	EXPECT_EQ(expectedNum, num);\
	if (expectedNum == num) {\
		log_success(testMessage);\
	} else {\
		log_fail(testMessage);\
	}\
	delete siter


	s_.OK();//ԭ����key�����ڣ�limit����1
	key = GetRandomKey_();
	limit = -1;
	SScanLoopProcess(limit, 0, "ԭ����key�����ڣ�limit����1");

	s_.OK();//keyΪ��ֵ��limint����1
	key = "";//ǰ���Ѿ�ɾ����keyΪ��ֵ�����������Ͳ�ɾ����
	limit = -1; 
	SScanLoopProcess(limit, 0, "keyΪ��ֵ��limint����1");

	s_.OK();//ԭ����key���ڣ���Ĳ���Set�ṹ���ݣ�limit����1
	key = GetRandomKey_();
	member = GetRandomVal_();
	limit = -1;
	SScanLoopProcess(limit, 0, "ԭ����key���ڣ���Ĳ���Set�ṹ���ݣ�limit����1");

	s_.OK();//ԭ����key���ڣ������Set���ݽṹ��limit����1
	num = 12;
	key = "SScan_Test";
	write_set(key, num);
	limit = -1;
	SScanLoopProcess(limit, num, "ԭ����key���ڣ������Set���ݽṹ��limit����1");

	s_.OK();//ԭ����key���ڣ������Set���ݽṹ��limitΪ������С��key���ϵ�Ԫ�ظ���
	limit = 10;
	SScanLoopProcess(limit, limit, "ԭ����key���ڣ������Set���ݽṹ��limitΪ������С��key���ϵ�Ԫ�ظ���");

	s_.OK();//ԭ����key���ڣ������Set���ݽṹ��limitΪ����������key���ϵ�Ԫ�ظ���
	limit = 30;
	SScanLoopProcess(limit, num, "ԭ����key���ڣ������Set���ݽṹ��limitΪ����������key���ϵ�Ԫ�ظ���");
}

TEST_F(NemoSetTest, TestSMembers) {
	log_message("\n========TestSScan========");
	
	string key, member;
	vector<string> members;
	uint64_t num;

	s_.OK();//ԭ����key�����ڣ�ԭ����key���ڣ���Ĳ���Set�ṹ����
	key = GetRandomKey_();
	members.clear();
	s_ = n_->SMembers(key, members);
	CHECK_STATUS(OK);
	EXPECT_EQ(true, members.empty());
	if (s_.ok() && members.empty()) {
		log_success("ԭ����key�����ڣ�ԭ����key���ڣ���Ĳ���Set�ṹ����");
	} else {
		log_fail("ԭ����key�����ڣ�ԭ����key���ڣ���Ĳ���Set�ṹ����");
	}

	s_.OK();//
	key = "SMembers_Test";
	num = 34;
	write_set(key, num);
	members.clear();
	s_ = n_->SMembers(key, members);
	CHECK_STATUS(OK);
	EXPECT_EQ(num, members.size());
	if (s_.ok() && num == members.size()) {
		log_success("ԭ����key�����ڣ�ԭ����key���ڣ���Ĳ���Set�ṹ����");
	} else {
		log_fail("ԭ����key�����ڣ�ԭ����key���ڣ���Ĳ���Set�ṹ����");
	}
	
	members.clear();
}

TEST_F(NemoSetTest, TestSUnionStore) {
	log_message("\n========TestSUnionStore========");

	string keyDst, member, keyTemp;
	vector<string> keys;
	int64_t numStart, numEnd, res;
	
	s_.OK();//destination�����ڣ�keys���ڣ������Ԫ�ز��غ�
	keyDst = "SUnionStore_Test_Dst";
	keyTemp = "SUnionStore_Test_Src1";
	numStart = 0;
	numEnd = 20;
	write_set_scope(keyTemp, numStart, numEnd);
	keys.push_back(keyTemp);
	keyTemp = "SUnionStore_Test_Src2";
	numStart = 20;
	numEnd = 40;
	write_set_scope(keyTemp, numStart, numEnd);
	keys.push_back(keyTemp);
	s_ = n_->SUnionStore(keyDst, keys, &res);
	CHECK_STATUS(OK);
	EXPECT_EQ(40, res);
	if (s_.ok() && 40 == res) {
		log_success("destination�����ڣ�keys���ڣ������Ԫ�ز��غ�");
	} else {
		log_fail("destination�����ڣ�keys���ڣ������Ԫ�ز��غ�");
	}

	s_.OK();//destination�����ڣ�keys���ڣ������Ԫ�����غ�
	keyDst = "SUnionStore_Test_Dst1";
	keys.clear();
	keyTemp = "SUnionStore_Test_Src3";
	numStart = 0;
	numEnd = 20;
	write_set_scope(keyTemp, numStart, numEnd);
	keys.push_back(keyTemp);
	keyTemp = "SUnionStore_Test_Src4";
	numStart = 10;
	numEnd = 30;
	write_set_scope(keyTemp, numStart, numEnd);
	keys.push_back(keyTemp);
	s_ = n_->SUnionStore(keyDst, keys, &res);
	CHECK_STATUS(OK);
	EXPECT_EQ(30, res);
	if (s_.ok() && 30 == res) {
		log_success("destination�����ڣ�keys���ڣ������Ԫ�����غ�");
	} else {
		log_fail("destination�����ڣ�keys���ڣ������Ԫ�����غ�");
	}

	s_.OK();//destination���ڣ�����SetԪ�أ�keys�����Ԫ�ز��غ�
	keys.clear();
	keyTemp = "SUnionStore_Test_Src5";
	numStart = 40;
	numEnd = 60;
	write_set_scope(keyTemp, numStart, numEnd);
	keys.push_back(keyTemp);
	keyTemp = "SUnionStore_Test_Src6";
	numStart = 60;
	numEnd = 80;
	write_set_scope(keyTemp, numStart, numEnd);
	keys.push_back(keyTemp);
	s_ = n_->SUnionStore(keyDst, keys, &res);
	CHECK_STATUS(OK);
	EXPECT_EQ(40, res);
	bool flagTemp = n_->SIsMember(keyDst, "0");
	EXPECT_EQ(false, flagTemp);//"SUnionStore_Test_Src3_0"��������һ�ε�keyDst��
	if (s_.ok() && 40==res && !flagTemp) {
		log_success("destination���ڣ�����SetԪ�أ�keys�����Ԫ�ز��غ�");
	} else {
		log_fail("destination���ڣ�����SetԪ�أ�keys�����Ԫ�ز��غ�");
	}

	keys.clear();
}

TEST_F(NemoSetTest, TestSUnion) {
	log_message("\n========TestSUnion========");

	string member, keyTemp;
	vector<string> keys, members;
	int64_t numStart, numEnd;

	s_.OK();//keys���в����ڵ�key�����ߴ��ڣ����Ǵ�Ĳ���Set�ṹ����
	keyTemp = GetRandomKey_();
	keys.push_back(keyTemp);
	keyTemp = GetRandomKey_();
	keys.push_back(keyTemp);
	members.clear();
	s_ = n_->SUnion(keys, members);
	CHECK_STATUS(OK);
	EXPECT_EQ(true, members.empty());
	if (s_.ok() && members.empty()) {
		log_success("keys���в����ڵ�key�����ߴ��ڣ����Ǵ�Ĳ���Set�ṹ����");
	} else {
		log_fail("keys���в����ڵ�key�����ߴ��ڣ����Ǵ�Ĳ���Set�ṹ����");
	}

	s_.OK();//destination�����ڣ�keys���ڣ������Ԫ�ز��غ�
	keyTemp = "SUnion_Test_Src1";
	numStart = 0;
	numEnd = 20;
	write_set_scope(keyTemp, numStart, numEnd);
	keys.push_back(keyTemp);
	keyTemp = "SUnion_Test_Src2";
	numStart = 20;
	numEnd = 40;
	write_set_scope(keyTemp, numStart, numEnd);
	keys.push_back(keyTemp);
	members.clear();
	s_ = n_->SUnion(keys, members);
	CHECK_STATUS(OK);
	EXPECT_EQ(40, members.size());
	if (s_.ok() && (uint64_t)(40) == members.size()) {
		log_success("destination�����ڣ�keys���ڣ������Ԫ�ز��غ�");
	} else {
		log_fail("destination�����ڣ�keys���ڣ������Ԫ�ز��غ�");
	}

	s_.OK();//destination�����ڣ�keys���ڣ������Ԫ�����غ�
	keys.clear();
	keyTemp = "SUnion_Test_Src3";
	numStart = 0;
	numEnd = 20;
	write_set_scope(keyTemp, numStart, numEnd);
	keys.push_back(keyTemp);
	keyTemp = "SUnion_Test_Src4";
	numStart = 10;
	numEnd = 30;
	write_set_scope(keyTemp, numStart, numEnd);
	keys.push_back(keyTemp);
	members.clear();
	s_ = n_->SUnion(keys, members);
	CHECK_STATUS(OK);
	EXPECT_EQ(30, members.size());
	if (s_.ok() && 30 == members.size()) {
		log_success("destination�����ڣ�keys���ڣ������Ԫ�����غ�");
	} else {
		log_fail("destination�����ڣ�keys���ڣ������Ԫ�����غ�");
	}

	keys.clear();
	members.clear();
}

TEST_F(NemoSetTest, TestSInterStore) {
	log_message("\n========TestSInterStore========");
	
	string keyTemp, keyDst; 
	vector<string> keys;
	int64_t res, numStart, numEnd, num;
	
#define SInterStoreLoopProcess(expectedState, expectedRes, testMessage)\
	s_ = n_->SInterStore(keyDst, keys, &res);\
	CHECK_STATUS(expectedState);\
	EXPECT_EQ(expectedRes, res);\
	if (string(#expectedState) == "OK") {\
		if (s_.ok() && res == expectedRes) {\
			log_success(testMessage);\
		} else {\
			log_fail(testMessage);\
		}\
	} else if (string(#expectedState)=="Corruption") {\
		if (s_.IsCorruption() && res == expectedRes) {\
			log_success(testMessage);\
		} else {\
			log_fail(testMessage);\
		}\
	}\
	do {} while(false)

	s_.OK();//destination�����ڣ�keysΪ�գ�
	keyDst = "SInterStore_Test_Dst1";
	keys.clear();
	res = -1;
	SInterStoreLoopProcess(Corruption, -1, "destination�����ڣ�keysΪ�գ�");

	s_.OK();//destination�����ڣ�keys����Ϊ1
	keyDst = "SInterStore_Test_Dst2";
	keyTemp = "SInterStore_Test_Src1";
	num = 13;
	write_set(keyTemp, num);
	keys.push_back(keyTemp);
	SInterStoreLoopProcess(OK, num, "destination�����ڣ�keys����Ϊ1");

	s_.OK();//destination�����ڣ�keys����Ϊ2�����ظ���Ԫ�أ�
	keys.clear();
	keyDst = "SInterStore_Test_Dst3";
	keyTemp = "SInterStore_Test_Src2";
	numStart = 0;
	numEnd = 20;
	write_set_scope(keyTemp, numStart, numEnd);
	keys.push_back(keyTemp);
	keyTemp = "SInterStore_Test_Src3";
	numStart = 10;
	numEnd = 40;
	write_set_scope(keyTemp, numStart, numEnd);
	keys.push_back(keyTemp);
	SInterStoreLoopProcess(OK, 10, "destination�����ڣ�keys����Ϊ2�����ظ���Ԫ�أ�");

	s_.OK();//destination�����ڣ�keys����Ϊ2��û���ظ���Ԫ��
	keys.clear();
	keyDst = "SInterStore_Test_Dst4";
	keyTemp = "SInterStore_Test_Src4";
	numStart = 0;
	numEnd = 20;
	write_set_scope(keyTemp, numStart, numEnd);
	keys.push_back(keyTemp);
	keyTemp = "SInterStore_Test_Src5";
	numStart = 20;
	numEnd = 40;
	write_set_scope(keyTemp, numStart, numEnd);
	keys.push_back(keyTemp);
	SInterStoreLoopProcess(OK, 0, "destination�����ڣ�keys����Ϊ2��û���ظ���Ԫ��");

	s_.OK();//destination�����ڣ�keys����>=3�����ظ���Ԫ��
	keys.clear();
	keyDst = "SInterStore_Test_Dst5";
	keyTemp = "SInterStore_Test_Src6";
	numStart = 0;
	numEnd = 30;
	write_set_scope(keyTemp, numStart, numEnd);
	keys.push_back(keyTemp);
	keyTemp = "SInterStore_Test_Src7";
	numStart = 20;
	numEnd = 40;
	write_set_scope(keyTemp, numStart, numEnd);
	keys.push_back(keyTemp);
	keyTemp = "SInterStore_Test_Src8";
	numStart = 10;
	numEnd = 50;
	write_set_scope(keyTemp, numStart, numEnd);
	keys.push_back(keyTemp);
	SInterStoreLoopProcess(OK, 10, "destination�����ڣ�keys����>=3�����ظ���Ԫ��");

	s_.OK();//destination�����ڣ�keys��������>=3���и����໥�ظ���Ԫ�ص�û��ȫ���ظ���Ԫ��
	keys.clear();
	keyDst = "SInterStore_Test_Dst6";
	keyTemp = "SInterStore_Test_Src9";
	numStart = 0;
	numEnd = 30;
	write_set_scope(keyTemp, numStart, numEnd);
	keys.push_back(keyTemp);
	keyTemp = "SInterStore_Test_Src10";
	numStart = 20;
	numEnd = 50;
	write_set_scope(keyTemp, numStart, numEnd);
	keys.push_back(keyTemp);
	keyTemp = "SInterStore_Test_Src11";
	numStart = 40;
	numEnd = 70;
	write_set_scope(keyTemp, numStart, numEnd);
	keys.push_back(keyTemp);
	SInterStoreLoopProcess(OK, 0, "destination�����ڣ�keys��������>=3���и����໥�ظ���Ԫ�ص�û��ȫ���ظ���Ԫ��");

	s_.OK();//destination���ڣ�keys����Ϊ2�����غϵ�Ԫ��
	keyDst = "SInterStore_Test_Dst7";
	num = 10;
	write_set(keyDst, num);
	keys.clear();
	keyTemp = "SInterStore_Test_Src12";
	numStart = 0;
	numEnd = 30;
	write_set_scope(keyTemp, numStart, numEnd);
	keys.push_back(keyTemp);
	keyTemp = "SInterStore_Test_Src13";
	numStart = 10;
	numEnd = 40;
	write_set_scope(keyTemp, numStart, numEnd);
	keys.push_back(keyTemp);
	s_ = n_->SInterStore(keyDst, keys, &res);
	CHECK_STATUS(OK);
	EXPECT_EQ(20, res);
	bool flag = n_->SIsMember(keyDst, keyDst+"_0");
	EXPECT_EQ(false, flag);
	if (s_.ok() && 20 == res && !flag) {
		log_success("destination���ڣ�keys����Ϊ2�����غϵ�Ԫ��");
	} else {
		log_success("destination���ڣ�keys����Ϊ2�����غϵ�Ԫ��");
	}

	keys.clear();
}

TEST_F(NemoSetTest, TestSInter) {
	log_message("\n========TestSInter========");

	string keyTemp;
	vector<string> members;
	vector<string> keys;
	int64_t numStart, numEnd, num;
	
#define SInterLoopProcess(expectedState, expectedSize, testMessage)\
	s_ = n_->SInter(keys, members);\
	CHECK_STATUS(expectedState);\
	EXPECT_EQ(expectedSize, members.size());\
	if (string(#expectedState) == "OK") {\
		if (s_.ok() && members.size() == expectedSize) {\
			log_success(testMessage);\
		} else {\
			log_fail(testMessage);\
		}\
	} else if (string(#expectedState)=="Corruption") {\
		if (s_.IsCorruption() && members.size() == expectedSize) {\
			log_success(testMessage);\
		} else {\
			log_fail(testMessage);\
		}\
	}\
	do {} while(false)

	s_.OK();//destination�����ڣ�keysΪ�գ�
	keys.clear();
	members.clear();
	SInterLoopProcess(Corruption, 0, "destination�����ڣ�keysΪ�գ�");

	s_.OK();//destination�����ڣ�keys����Ϊ1
	keyTemp = "SInter_Test_Src1";
	num = 13;
	write_set(keyTemp, num);
	keys.push_back(keyTemp);
	members.clear();
	SInterLoopProcess(OK, num, "destination�����ڣ�keys����Ϊ1");

	s_.OK();//destination�����ڣ�keys����Ϊ2�����ظ���Ԫ�أ�
	keys.clear();
	keyTemp = "SInter_Test_Src2";
	numStart = 0;
	numEnd = 20;
	write_set_scope(keyTemp, numStart, numEnd);
	keys.push_back(keyTemp);
	keyTemp = "SInter_Test_Src3";
	numStart = 10;
	numEnd = 40;
	write_set_scope(keyTemp, numStart, numEnd);
	keys.push_back(keyTemp);
	members.clear();
	SInterLoopProcess(OK, 10, "destination�����ڣ�keys����Ϊ2�����ظ���Ԫ�أ�");

	s_.OK();//destination�����ڣ�keys����Ϊ2��û���ظ���Ԫ��
	keys.clear();
	keyTemp = "SInter_Test_Src4";
	numStart = 0;
	numEnd = 20;
	write_set_scope(keyTemp, numStart, numEnd);
	keys.push_back(keyTemp);
	keyTemp = "SInter_Test_Src5";
	numStart = 20;
	numEnd = 40;
	write_set_scope(keyTemp, numStart, numEnd);
	keys.push_back(keyTemp);
	members.clear();
	SInterLoopProcess(OK, 0, "destination�����ڣ�keys����Ϊ2��û���ظ���Ԫ��");

	s_.OK();//destination�����ڣ�keys����>=3�����ظ���Ԫ��
	keys.clear();
	keyTemp = "SInter_Test_Src6";
	numStart = 0;
	numEnd = 30;
	write_set_scope(keyTemp, numStart, numEnd);
	keys.push_back(keyTemp);
	keyTemp = "SInter_Test_Src7";
	numStart = 20;
	numEnd = 40;
	write_set_scope(keyTemp, numStart, numEnd);
	keys.push_back(keyTemp);
	keyTemp = "SInter_Test_Src8";
	numStart = 10;
	numEnd = 50;
	write_set_scope(keyTemp, numStart, numEnd);
	keys.push_back(keyTemp);
	members.clear();
	SInterLoopProcess(OK, 10, "destination�����ڣ�keys����>=3�����ظ���Ԫ��");

	s_.OK();//destination�����ڣ�keys��������>=3���и����໥�ظ���Ԫ�ص�û��ȫ���ظ���Ԫ��
	keys.clear();
	keyTemp = "SInter_Test_Src9";
	numStart = 0;
	numEnd = 30;
	write_set_scope(keyTemp, numStart, numEnd);
	keys.push_back(keyTemp);
	keyTemp = "SInter_Test_Src10";
	numStart = 20;
	numEnd = 50;
	write_set_scope(keyTemp, numStart, numEnd);
	keys.push_back(keyTemp);
	keyTemp = "SInter_Test_Src11";
	numStart = 40;
	numEnd = 70;
	write_set_scope(keyTemp, numStart, numEnd);
	keys.push_back(keyTemp);
	members.clear();
	SInterLoopProcess(OK, 0, "destination�����ڣ�keys��������>=3���и����໥�ظ���Ԫ�ص�û��ȫ���ظ���Ԫ��");
	
	keys.clear();
	members.clear();
}

TEST_F(NemoSetTest, TestSDiffStore) {
	log_message("\n========TestSDiffStore========");
	
	string keyTemp, keyDst; 
	vector<string> keys;
	int64_t res, numStart, numEnd, num;
	
#define SDiffStoreLoopProcess(expectedState, expectedRes, testMessage)\
	s_ = n_->SDiffStore(keyDst, keys, &res);\
	CHECK_STATUS(expectedState);\
	EXPECT_EQ(expectedRes, res);\
	if (string(#expectedState) == "OK") {\
		if (s_.ok() && res == expectedRes) {\
			log_success(testMessage);\
		} else {\
			log_fail(testMessage);\
		}\
	} else if (string(#expectedState)=="Corruption") {\
		if (s_.IsCorruption() && res == expectedRes) {\
			log_success(testMessage);\
		} else {\
			log_fail(testMessage);\
		}\
	}\
	do {} while(false)

	s_.OK();//destination�����ڣ�keysΪ�գ�
	keyDst = "SDiffStore_Test_Dst1";
	keys.clear();
	res = -1;
	SDiffStoreLoopProcess(Corruption, -1, "destination�����ڣ�keysΪ�գ�");

	s_.OK();//destination�����ڣ�keys����Ϊ1
	keyDst = "SDiffStore_Test_Dst2";
	keyTemp = "SDiffStore_Test_Src1";
	num = 23;
	write_set(keyTemp, num);
	keys.push_back(keyTemp);
	SDiffStoreLoopProcess(OK, num, "destination�����ڣ�keys����Ϊ1");

	s_.OK();//destination�����ڣ�keys����Ϊ2���в��ظ���Ԫ�أ�
	keys.clear();
	keyDst = "SDiffStore_Test_Dst3";
	keyTemp = "SDiffStore_Test_Src2";
	numStart = 0;
	numEnd = 30;
	write_set_scope(keyTemp, numStart, numEnd);
	keys.push_back(keyTemp);
	keyTemp = "SDiffStore_Test_Src3";
	numStart = 10;
	numEnd = 40;
	write_set_scope(keyTemp, numStart, numEnd);
	keys.push_back(keyTemp);
	SDiffStoreLoopProcess(OK, 10, "destination�����ڣ�keys����Ϊ2���в��ظ���Ԫ�أ�");

	s_.OK();//destination�����ڣ�keys����Ϊ2��û�в��ظ���Ԫ��
	keys.clear();
	keyDst = "SDiffStore_Test_Dst4";
	keyTemp = "SDiffStore_Test_Src4";
	numStart = 0;
	numEnd = 20;
	write_set_scope(keyTemp, numStart, numEnd);
	keys.push_back(keyTemp);
	keyTemp = "SDiffStore_Test_Src5";
	numStart = 0;
	numEnd = 20;
	write_set_scope(keyTemp, numStart, numEnd);
	keys.push_back(keyTemp);
	SDiffStoreLoopProcess(OK, 0, "destination�����ڣ�keys����Ϊ2��û���ظ���Ԫ��");

	s_.OK();//destination�����ڣ�keys����>=3������֮��ȫ�����໥�ظ�
	keys.clear();
	keyDst = "SDiffStore_Test_Dst5";
	keyTemp = "SDiffStore_Test_Src6";
	numStart = 0;
	numEnd = 20;
	write_set_scope(keyTemp, numStart, numEnd);
	keys.push_back(keyTemp);
	keyTemp = "SDiffStore_Test_Src7";
	numStart = 20;
	numEnd = 40;
	write_set_scope(keyTemp, numStart, numEnd);
	keys.push_back(keyTemp);
	keyTemp = "SDiffStore_Test_Src8";
	numStart = 40;
	numEnd = 60;
	write_set_scope(keyTemp, numStart, numEnd);
	keys.push_back(keyTemp);
	SDiffStoreLoopProcess(OK, 20, "destination�����ڣ�keys����>=3�����ظ���Ԫ��");

	s_.OK();//destination�����ڣ�keys��������>=3���и����໥�ظ���Ԫ�ص�û��ȫ���ظ���Ԫ��
	keys.clear();
	keyDst = "SDiffStore_Test_Dst6";
	keyTemp = "SDiffStore_Test_Src9";
	numStart = 0;
	numEnd = 60;
	write_set_scope(keyTemp, numStart, numEnd);
	keys.push_back(keyTemp);
	keyTemp = "SDiffStore_Test_Src10";
	numStart = 20;
	numEnd = 50;
	write_set_scope(keyTemp, numStart, numEnd);
	keys.push_back(keyTemp);
	keyTemp = "SDiffStore_Test_Src11";
	numStart = 10;
	numEnd = 35;
	write_set_scope(keyTemp, numStart, numEnd);
	keys.push_back(keyTemp);
	SDiffStoreLoopProcess(OK, 20, "destination�����ڣ�keys��������>=3���и����໥�ظ���Ԫ�ص�û��ȫ���ظ���Ԫ��");

	s_.OK();//destination���ڣ�keys����Ϊ2�����غϵ�Ԫ��
	keyDst = "SDiffStore_Test_Dst7";
	num = 10;
	write_set(keyDst, num);
	keys.clear();
	keyTemp = "SDiffStore_Test_Src12";
	numStart = 0;
	numEnd = 30;
	write_set_scope(keyTemp, numStart, numEnd);
	keys.push_back(keyTemp);
	keyTemp = "SDiffStore_Test_Src13";
	numStart = 10;
	numEnd = 40;
	write_set_scope(keyTemp, numStart, numEnd);
	keys.push_back(keyTemp);
	s_ = n_->SDiffStore(keyDst, keys, &res);
	CHECK_STATUS(OK);
	EXPECT_EQ(10, res);
	bool flag = n_->SIsMember(keyDst, keyDst+"_0");
	EXPECT_EQ(false, flag);
	if (s_.ok() && 10 == res && !flag) {
		log_success("destination���ڣ�keys����Ϊ2�����غϵ�Ԫ��");
	} else {
		log_success("destination���ڣ�keys����Ϊ2�����غϵ�Ԫ��");
	}
	
	keys.clear();
}

TEST_F(NemoSetTest, TestSDiff) {
	log_message("\n========TestSDiff========");
	
	string keyTemp;
	vector<string> keys, members;
	int64_t numStart, numEnd, num;
	
#define SDiffLoopProcess(expectedState, expectedSize, testMessage)\
	s_ = n_->SDiff(keys, members);\
	CHECK_STATUS(expectedState);\
	EXPECT_EQ(expectedSize, members.size());\
	if (string(#expectedState) == "OK") {\
		if (s_.ok() && members.size() == expectedSize) {\
			log_success(testMessage);\
		} else {\
			log_fail(testMessage);\
		}\
	} else if (string(#expectedState)=="Corruption") {\
		if (s_.IsCorruption() && members.size() == expectedSize) {\
			log_success(testMessage);\
		} else {\
			log_fail(testMessage);\
		}\
	}\
	do {} while(false)

	s_.OK();//destination�����ڣ�keysΪ�գ�
	keys.clear();
	members.clear();
	SDiffLoopProcess(Corruption, 0, "destination�����ڣ�keysΪ�գ�");

	s_.OK();//destination�����ڣ�keys����Ϊ1
	keyTemp = "SInterDiff_Test_Src1";
	num = 27;
	write_set(keyTemp, num);
	keys.push_back(keyTemp);
	members.clear();
	SDiffLoopProcess(OK, num, "destination�����ڣ�keys����Ϊ1");

	s_.OK();//destination�����ڣ�keys����Ϊ2���в��ظ���Ԫ�أ�
	keys.clear();
	keyTemp = "SDiff_Test_Src2";
	numStart = 0;
	numEnd = 35;
	write_set_scope(keyTemp, numStart, numEnd);
	keys.push_back(keyTemp);
	keyTemp = "SDiff_Test_Src3";
	numStart = 10;
	numEnd = 49;
	write_set_scope(keyTemp, numStart, numEnd);
	keys.push_back(keyTemp);
	members.clear();
	SDiffLoopProcess(OK, 10, "destination�����ڣ�keys����Ϊ2���в��ظ���Ԫ�أ�");

	s_.OK();//destination�����ڣ�keys����Ϊ2��û�в��ظ���Ԫ��
	keys.clear();
	keyTemp = "SDiff_Test_Src4";
	numStart = 0;
	numEnd = 29;
	write_set_scope(keyTemp, numStart, numEnd);
	keys.push_back(keyTemp);
	keyTemp = "SDiff_Test_Src5";
	numStart = 0;
	numEnd = 29;
	write_set_scope(keyTemp, numStart, numEnd);
	keys.push_back(keyTemp);
	members.clear();
	SDiffLoopProcess(OK, 0, "destination�����ڣ�keys����Ϊ2��û���ظ���Ԫ��");

	s_.OK();//destination�����ڣ�keys����>=3������֮��ȫ�����໥�ظ�
	keys.clear();
	keyTemp = "SDiff_Test_Src6";
	numStart = 0;
	numEnd = 17;
	write_set_scope(keyTemp, numStart, numEnd);
	keys.push_back(keyTemp);
	keyTemp = "SDiff_Test_Src7";
	numStart = 20;
	numEnd = 40;
	write_set_scope(keyTemp, numStart, numEnd);
	keys.push_back(keyTemp);
	keyTemp = "SDiff_Test_Src8";
	numStart = 40;
	numEnd = 60;
	write_set_scope(keyTemp, numStart, numEnd);
	keys.push_back(keyTemp);
	members.clear();
	SDiffLoopProcess(OK, 17, "destination�����ڣ�keys����>=3�����ظ���Ԫ��");

	s_.OK();//destination�����ڣ�keys��������>=3���и����໥�ظ���Ԫ�ص�û��ȫ���ظ���Ԫ��
	keys.clear();
	keyTemp = "SDiff_Test_Src9";
	numStart = 0;
	numEnd = 49;
	write_set_scope(keyTemp, numStart, numEnd);
	keys.push_back(keyTemp);
	keyTemp = "SDiff_Test_Src10";
	numStart = 20;
	numEnd = 50;
	write_set_scope(keyTemp, numStart, numEnd);
	keys.push_back(keyTemp);
	keyTemp = "SDiff_Test_Src11";
	numStart = 10;
	numEnd = 35;
	write_set_scope(keyTemp, numStart, numEnd);
	keys.push_back(keyTemp);
	members.clear();
	SDiffLoopProcess(OK, 10, "destination�����ڣ�keys��������>=3���и����໥�ظ���Ԫ�ص�û��ȫ���ظ���Ԫ��");

	keys.clear();
	members.clear();
}

TEST_F(NemoSetTest, TestSIsMember) {
	log_message("\n========TestSDiff========");
	
	string key, member;
	int64_t res;
	bool retBool;

	s_.OK();//key������
	key = GetRandomKey_();
	member = GetRandomVal_();
	retBool = n_->SIsMember(key, member);
	EXPECT_EQ(false, retBool);
	if (false == retBool) {
		log_success("key������");
	} else {
		log_fail("key������");
	}

	s_.OK();//key���Set���ݽṹ,�Ұ���member
	n_->SAdd(key, member, &res);
	retBool = n_->SIsMember(key, member);
	EXPECT_EQ(true, retBool);
	if (true == retBool) {
		log_success("key���Set���ݽṹ,�Ұ���member");
	} else {
		log_fail("key���Set���ݽṹ,�Ұ���member");
	}

	s_.OK();//"key���Set���ݽṹ������member"
	member = GetRandomVal_();
	retBool = n_->SIsMember(key, member);
	EXPECT_EQ(false, retBool);
	if (false == retBool) {
		log_success("key���Set���ݽṹ������member");
	} else {
		log_fail("key���Set���ݽṹ������member");
	}
}

TEST_F(NemoSetTest, TestSPop) {
	log_message("\n========TestSPop========");

	string key, member, getMember;
	int64_t num, card, resTemp;
	bool flag;

	s_.OK();//key�����ڣ�key���ڣ����Ǵ�Ĳ���Set�ṹ
	key = GetRandomKey_();
	member = GetRandomVal_();
	s_ = n_->SPop(key, getMember);
	CHECK_STATUS(NotFound);
	if (s_.IsNotFound()) {
		log_success("key�����ڣ�key���ڣ����Ǵ�Ĳ���Set�ṹ");
	} else {
		log_fail("key�����ڣ�key���ڣ����Ǵ�Ĳ���Set�ṹ");
	}

	s_.OK();//key���ڣ������Set�ṹ
	key = GetRandomKey_();
	nemo::SIterator* siter = n_->SScan(key, -1);
	while (siter->Next()) {
		n_->SRem(key, siter->Member(), &resTemp);
	}
	delete siter;
	member = GetRandomVal_();
	n_->SAdd(key, member, &resTemp);
	s_ = n_->SPop(key, getMember);
	CHECK_STATUS(OK);
	EXPECT_EQ(member, getMember);
	if (s_.ok() && member == getMember) {
		log_success("key���ڣ������Set�ṹ");
	} else {
		log_fail("key���ڣ������Set�ṹ");
	}

	s_.OK();//key���ڣ������Set�ṹ�����Ƿ����SPop��
	key = "SPop_Test";
	num = 10;
	write_set(key, num);
	flag = true;
	while (num>0) {
		s_ = n_->SPop(key, getMember);
		if (!s_.ok()) {
			flag = false;
			break;
		}
		num--;
	}
	card = n_->SCard(key);
	EXPECT_EQ(true, flag);
	EXPECT_EQ(0, card);
	if (flag && 0 == card) {
		log_success("key���ڣ������Set�ṹ�����Ƿ����SPop��");
	} else {
		log_fail("key���ڣ������Set�ṹ�����Ƿ����SPop��");
	}
}

TEST_F(NemoSetTest, TestSRandMember) {
	log_message("========SRandMember========");

	string key;
	vector<string> members;
	int64_t count, card, num;
	bool isUniqueFlag, isAllInKeyFlag;

	s_.OK();//keyΪ�գ�key���ڣ����Ǵ�Ĳ���Set�ṹ
	members.clear();
	key = GetRandomKey_();
	s_ = n_->SRandMember(key, members);
	CHECK_STATUS(NotFound);
	EXPECT_EQ(true, members.empty());
	if (s_.IsNotFound() && members.empty()) {
		log_success("keyΪ�գ�key���ڣ����Ǵ�Ĳ���Set�ṹ");
	} else {
		log_fail("keyΪ�գ�key���ڣ����Ǵ�Ĳ���Set�ṹ");
	}

	key = "SRandMember_Test";
	num = 50;
	write_set(key, num);

	s_.OK();//key���ڣ�count��0��
	count = 0;
	members.clear();
	s_ = n_->SRandMember(key, members, count);
	CHECK_STATUS(OK);
	EXPECT_EQ(true, members.empty());
	if (s_.ok() && members.empty()) {
		log_success("key���ڣ�count��0");
	} else {
		log_fail("key���ڣ�count��0");
	}

	s_.OK();//key���ڣ�count>card>0
	count = GetRandomUint_(1, num-1);
	members.clear();
	s_ = n_->SRandMember(key, members, count);
	CHECK_STATUS(OK);
	EXPECT_EQ(count, members.size());
	isUniqueFlag = isUnique(members);
	isAllInKeyFlag = isAllInKey(key, members);
	EXPECT_EQ(true, isUniqueFlag);
	EXPECT_EQ(true, isAllInKeyFlag);
	if (s_.ok() && count == members.size() && isUniqueFlag && isAllInKeyFlag) {
		log_success("key���ڣ�count>card>0");
	} else {
		log_fail("key���ڣ�count>card>0");
	}
	
	s_.OK();//key���ڣ�0<count<card
	count = num+13;
	members.clear();
	s_ = n_->SRandMember(key, members, count);
	CHECK_STATUS(OK);
	EXPECT_EQ(num, members.size());
	isUniqueFlag = isUnique(members);
	isAllInKeyFlag = isAllInKey(key, members);
	EXPECT_EQ(true, isUniqueFlag);
	EXPECT_EQ(true, isAllInKeyFlag);
	if (s_.ok() && num == members.size() && isUniqueFlag && isAllInKeyFlag) {
		log_success("key���ڣ�count>card>0");
	} else {
		log_fail("key���ڣ�count>card>0");
	}
	
	s_.OK();//key���ڣ���card<count<0
	count = GetRandomUint_(1, num-1);
	count = (-1)*count;
	members.clear();
	s_ = n_->SRandMember(key, members, count);
	CHECK_STATUS(OK);
	EXPECT_EQ((-1)*count, members.size());
	isAllInKeyFlag = isAllInKey(key, members);
	EXPECT_EQ(true, isAllInKeyFlag);
	if (s_.ok() && (-1)*count == members.size() && isAllInKeyFlag) {
		log_success("key���ڣ���card<count<0");
	} else {
		log_fail("key���ڣ���card<count<0");
	}

	s_.OK();//key���ڣ�count<-card
	count = num+20;
	count = (-1)*count;
	members.clear();
	s_ = n_->SRandMember(key, members, count);
	CHECK_STATUS(OK);
	EXPECT_EQ((-1)*count, members.size());
	isUniqueFlag = isUnique(members);
	isAllInKeyFlag = isAllInKey(key, members);
	EXPECT_EQ(false, isUniqueFlag);
	EXPECT_EQ(true, isAllInKeyFlag);
	if (s_.ok() && (-1)*count == members.size() && (!isUniqueFlag) && isAllInKeyFlag) {
		log_success("key���ڣ�count<-card");
	} else {
		log_fail("key���ڣ�count<-card");
	}
}

TEST_F(NemoSetTest, TestSMove) {
	log_message("\n========TestSMove========");
	
	string keySrc, keyDst, member;
	int64_t res, resTemp, cardSrc, cardDst, numStart, numEnd;

	s_.OK();//srcKey������
	keySrc = GetRandomKey_();
	keyDst = GetRandomKey_();
	member = GetRandomVal_();
	s_ = n_->SMove(keySrc, keyDst, member, &res);
	CHECK_STATUS(NotFound);
	EXPECT_EQ(0, res);
	if (s_.IsNotFound() && res == 0) {
		log_success("srcKey������");
	} else {
		log_fail("srcKey������");
	}

	s_.OK();//srcKey���ڣ�����û��member
	n_->SAdd(keySrc, member, &resTemp);
	n_->SRem(keySrc, member, &resTemp);
	s_ = n_->SMove(keySrc, keyDst, member, &res);
	CHECK_STATUS(NotFound);
	EXPECT_EQ(0, res);
	if (s_.IsNotFound() && res == 0) {
		log_success("srcKey���ڣ�����û��member");
	} else {
		log_fail("srcKey���ڣ�����û��member");
	}

	s_.OK();//srcKey���ڣ�����member��dstKey������
	keySrc = GetRandomKey_();
	member = GetRandomVal_();
	keyDst = GetRandomKey_();
	n_->SAdd(keySrc, member, &resTemp);
	s_ = n_->SMove(keySrc, keyDst, member, &res);
	CHECK_STATUS(OK);
	EXPECT_EQ(1, res);
	cardSrc = n_->SCard(keySrc);
	cardDst = n_->SCard(keyDst);
	EXPECT_EQ(0, cardSrc);
	EXPECT_EQ(1, cardDst);
	if (s_.ok() && res==1 && 0==cardSrc && 1==cardDst) {
		log_success("srcKey���ڣ�����member��dstKey������");
	} else {
		log_fail("srcKey���ڣ�����member��dstKey������");
	}

	s_.OK();//srcKey���ڣ�����member��dstKey���ڣ�����û��member
	keySrc = "SMove_Test_Src1";
	numStart = 0;
	numEnd = 10;
	write_set_scope(keySrc, numStart, numEnd);
	keyDst = "SMove_Test_Dst1";
	numStart = 10;
	numEnd = 20;
	write_set_scope(keyDst, numStart, numEnd);
	s_ = n_->SMove(keySrc, keyDst, itoa(GetRandomUint_(0, 9)), &res);
	CHECK_STATUS(OK);
	EXPECT_EQ(1, res);
	cardSrc = n_->SCard(keySrc);
	cardDst = n_->SCard(keyDst);
	EXPECT_EQ(9, cardSrc);
	EXPECT_EQ(11, cardDst);
	if (s_.ok() && res==1 && 9==cardSrc&& 11==cardDst) {
		log_success("srcKey���ڣ�����member��dstKey���ڣ�����û��member");
	} else {
		log_fail("srcKey���ڣ�����member��dstKey���ڣ�����û��member");
	}

	s_.OK();//srdKey���ڣ�����member��dst���ڣ�����member
	keySrc = "SMove_Test_Src2";
	keyDst = "SMove_Test_Dst2";
	numStart = 10;
	numEnd = 20;
	write_set_scope(keySrc, numStart, numEnd);
	write_set_scope(keyDst, numStart, numEnd);
	s_ = n_->SMove(keySrc, keyDst, itoa(GetRandomUint_(numStart, numEnd-1)), &res);
	CHECK_STATUS(OK);
	EXPECT_EQ(1, res);
	cardSrc = n_->SCard(keySrc);
	cardDst = n_->SCard(keyDst);
	EXPECT_EQ(9, cardSrc);
	EXPECT_EQ(10, cardDst);
	if (s_.ok() && res==1 && 9 == cardSrc && 10 == cardDst) {
		log_success("srdKey���ڣ�����member��dst���ڣ�����member");
	} else {
		log_fail("srdKey���ڣ�����member��dst���ڣ�����member");
	}
	log_message("============================SETTEST END===========================");
	log_message("============================SETTEST END===========================\n\n");
}
