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
#include "nemo_hash_test.h"
using namespace std;


TEST_F(NemoHashTest, TestHSet)
{
	log_message("============================HASHTEST START===========================");
	log_message("============================HASHTEST START===========================");
#define LoopPositiveProcess(TestMessage) s_ = n_->HSet(key, field, val);\
					CHECK_STATUS(OK);\
    				n_->HGet(key, field, &getVal);\
					EXPECT_EQ(val, getVal);\
					if(s_.ok() && val == getVal)\
						log_success(TestMessage);\
					else\
						log_fail(TestMessage)
	log_message("========TestHSet========");
	string key, field, val, getVal;
	key = GetRandomKey_();
	field = GetRandomField_();
	val = GetRandomVal_();

	s_.OK();//����key������
	s_ = n_->HSet(key, field, val);
	CHECK_STATUS(OK);
	n_->HGet(key, field, &getVal);
	EXPECT_EQ(val, getVal);
	if(s_.ok() && val == getVal)
		log_success("����key�����ڵ����");
	else
		log_fail("����key�����ڵ����");

	s_.OK();//����key���ڵ����
	field = GetRandomField_();
	val = GetRandomVal_();
	s_ = n_->HSet(key, field, val);
	CHECK_STATUS(OK);
	n_->HGet(key, field, &getVal);
	EXPECT_EQ(val, getVal);
	if(s_.ok() && val == getVal)
		log_success("����key���ڵ����");
	else
		log_fail("����key���ڵ����");

	s_.OK();//����key���ڵ������field���ڵ����
	val = GetRandomVal_();
	s_ = n_->HSet(key, field, val);
	CHECK_STATUS(OK);
	n_->HGet(key, field, &getVal);
	EXPECT_EQ(val, getVal);
	if(s_.ok() && val == getVal)
		log_success("����key���ڵ����");
	else
		log_fail("����key���ڵ����");
	
	s_.OK();//����valȡ��󳤶�
	val = GetRandomBytes_(maxValLen_);
	LoopPositiveProcess("����valȡ��󳤶�");

	s_.OK();//����valȡ��ֵ
	val = "";
	LoopPositiveProcess("����valȡ��ֵ");
	
	s_.OK();//����fieldȡ��󳤶�
	val = GetRandomVal_();
	field = GetRandomBytes_(maxFieldLen_);
	LoopPositiveProcess("����fieldȡ��󳤶�");

	s_.OK();//����fieldΪ��
	val = GetRandomVal_();
	field = "";
	LoopPositiveProcess("����fieldΪ��");
	
	s_.OK();//����key��󳤶�
	field = GetRandomField_();
	key = GetRandomBytes_(maxKeyLen_);
	LoopPositiveProcess("����key��󳤶�");

	s_.OK();//����keyΪ��
	key = "";
	field = GetRandomField_();
	val = GetRandomVal_();
	LoopPositiveProcess("����fieldΪ��");
}

TEST_F(NemoHashTest, TestHGet)
{
	log_message("\n========TestHSet========");
	
	#define GetLoopOKProcess(TestMessage) s_ = n_->HGet(key, field, &getVal);\
									  CHECK_STATUS(OK);\
									  EXPECT_EQ(val, getVal);\
									  if(s_.ok() && val == getVal)\
										  log_success(TestMessage);\
									  else\
										  log_fail(TestMessage)
	#define GetLoopNotFoundProcess(TestMessage) getVal = "";\
									  s_ = n_->HGet(key, field, &getVal);\
									  CHECK_STATUS(NotFound);\
									  EXPECT_EQ(true, getVal.empty());\
									  if(s_.IsNotFound() && getVal.empty())\
										  log_success(TestMessage);\
									  else\
										  log_fail(TestMessage)


	string key, field, val, getVal;

	s_.OK();//ԭ����key�����ڣ�field��/���ڣ�ԭ����val�ǿ�
	key = GetRandomKey_();
	field = GetRandomField_();
	val = GetRandomVal_();
	n_->HDel(key, field);
	GetLoopNotFoundProcess("ԭ����key�����ڣ�field��/���ڣ�ԭ����val�ǿ�");

	s_.OK();//ԭ����key���ڣ�field���ڣ�ԭ����val�ǿ�
	key = GetRandomKey_();
	field = GetRandomField_();
	val = GetRandomVal_();
	n_->HSet(key, field, val);
	GetLoopOKProcess("ԭ����key���ڣ�field���ڣ�ԭ����val�ǿ�");
	
	s_.OK();//ԭ����key���ڣ�field�����ڣ�ԭ����val�ǿ�
	field = GetRandomField_();
	val = GetRandomVal_();
	n_->HSet(key, field, val);
	n_->HDel(key, field);
	GetLoopNotFoundProcess("ԭ����key���ڣ�field�����ڣ�ԭ����val�ǿ�");

	s_.OK();//ԭ����key���ڣ�field���ڣ�ԭ����valΪ��
	val = "";
	n_->HSet(key, field, val);
	GetLoopOKProcess("ԭ����key���ڣ�field���ڣ�ԭ����valΪ��");

	s_.OK();//ԭ����key���ڣ�field���ڣ�ԭ����valȡ��󳤶�
	val = GetRandomBytes_(maxValLen_);
	n_->HSet(key, field, val);
	GetLoopOKProcess("ԭ����key���ڣ�field���ڣ�ԭ����valȡ��󳤶�");	
}

TEST_F(NemoHashTest, TestHDel)
{
	log_message("\n========TestHDel========");
	string key1, field1, val1, key, field, val;
	
	s_.OK();//ԭ����key������
	key = GetRandomKey_();
	field = GetRandomField_();
	s_ = n_->HDel(key, field);
	CHECK_STATUS(NotFound);
	if(s_.IsNotFound())
		log_success("ԭ����key������");
	else
		log_fail("ԭ����key������");
	
	s_.OK();//ԭ����key���ڣ�field����
	val = GetRandomVal_();
	n_->HSet(key, field, val);
    s_ = n_->HDel(key, field);
	CHECK_STATUS(OK);
	if(s_.ok())
		log_success("ԭ����key���ڣ�field����");
	else
		log_fail("ԭ����key���ڣ�field����");

	s_.OK();//ԭ����key���ڣ�field������
	s_ = n_->HDel(key, field);
	CHECK_STATUS(NotFound);
	if(s_.IsNotFound())
		log_success("ԭ����key���ڣ�field������");
	else
		log_fail("ԭ����key���ڣ�field������");
}

TEST_F(NemoHashTest, TestHExist)
{
	#define HExistLoopPositiveProcess(Message) isExist = n_->HExists(key, field);\
										EXPECT_EQ(true, isExist);\
										if(isExist)\
											log_success(Message);\
										else\
											log_fail(Message)
	#define HExistLoopNegativeProcess(Message) isExist = n_->HExists(key, field);\
										EXPECT_EQ(false, isExist);\
										if(!isExist)\
											log_success(Message);\
										else\
											log_fail(Message)
	log_message("\n========TestHExist========");
	string key, field, val;
	bool isExist;
	
	s_.OK();//ԭ����key������
	key = GetRandomKey_();
	field = GetRandomField_();
	n_->HDel(key, field);
	HExistLoopNegativeProcess("ԭ����key������");

	s_.OK();//ԭ����key���ڣ�field���ڣ�valΪ��
	val = "";
	n_->HSet(key, field, val);
	HExistLoopPositiveProcess("ԭ����key���ڣ�field���ڣ�valΪ��");

	s_.OK();//ԭ����key���ڣ�field���ڣ�val�ǿ�
	val = GetRandomVal_();
	n_->HSet(key, field, val);
	HExistLoopPositiveProcess("ԭ����key���ڣ�field���ڣ�valΪ��");
	
	s_.OK();//
	n_->HDel(key, field);
	HExistLoopNegativeProcess("ԭ����key���ڣ�field������");
}

TEST_F(NemoHashTest, TestHKeys)
{
	log_message("\n========TestHKeys========");
	string key, field, val;
	vector<string> fields;

	s_.OK();//key������
	key = GetRandomKey_();
	s_ = n_->HKeys(key, fields);
	CHECK_STATUS(OK);
	EXPECT_EQ(true, fields.empty());
	if(s_.ok() && true == fields.empty())
		log_success("key������");
	else
		log_fail("key������");

	s_.OK();//key���ڣ�û��field��
	fields.clear();
	field = GetRandomKey_();
	val = GetRandomVal_();
	n_->HSet(key, field, val);
	n_->HDel(key, field);
	s_ = n_->HKeys(key, fields);
	CHECK_STATUS(OK);
	EXPECT_EQ(true, fields.empty());
	if(s_.ok() && true == fields.empty())
		log_success("key���ڣ�û��field");
	else
		log_fail("key���ڣ�û��field");

	s_.OK();//key���ڣ���field��
	key = "HKey_Test";
	int fieldsNum = 100, fieldsIndexStart = 0;
	log_message("======����keyΪ%s, field��%s%d��%s%d", key.c_str(), (key+"_").c_str(), fieldsIndexStart, (key+"_").c_str(), fieldsIndexStart+fieldsNum-1);
	for(int fieldsIndex = fieldsIndexStart; fieldsIndex != fieldsIndexStart + fieldsNum; fieldsIndex++)
	{
		n_->HSet(key, key+"_"+itoa(fieldsIndex), itoa(fieldsIndex));
	}
	fields.clear();
	s_ = n_->HKeys(key, fields);
	CHECK_STATUS(OK);
	EXPECT_EQ(key+"_"+itoa(fieldsIndexStart), *(fields.begin()));
	EXPECT_EQ(key+"_"+itoa(fieldsIndexStart+fieldsNum-1), *(fields.rbegin()));
	EXPECT_EQ(fieldsNum, fields.size());
	if(s_.ok() && *(fields.begin()) == key+"_"+itoa(fieldsIndexStart) && key+"_"+itoa(fieldsIndexStart+fieldsNum-1) == *(fields.rbegin()) && fieldsNum == fields.size())
		log_success("key���ڣ���field");
	else
		log_fail("key���ڣ���field");

	s_.OK();//key���ڣ���fields������key֮���б�Ľṹ��keyxx
	val = GetRandomVal_();
	n_->Set(key+"1", val);
	val = GetRandomVal_();
	n_->Set(key+"2", val);
	fields.clear();
	s_ = n_->HKeys(key, fields);
	CHECK_STATUS(OK);
	EXPECT_EQ(key+"_"+itoa(fieldsIndexStart), *(fields.begin()));
	EXPECT_EQ(key+"_"+itoa(fieldsIndexStart+fieldsNum-1), *(fields.rbegin()));
	EXPECT_EQ(fieldsNum, fields.size());
	if(s_.ok() && *(fields.begin()) == key+"_"+itoa(fieldsIndexStart) && key+"_"+itoa(fieldsIndexStart+fieldsNum-1) == *(fields.rbegin()) && fieldsNum == fields.size())
		log_success("key���ڣ���fields������key֮���б�Ľṹ��keyxx");
	else
		log_fail("key���ڣ���fields������key֮���б�Ľṹ��keyxx");

	s_.OK();//key���ڣ����ǲ���hash�ṹ
	key = GetRandomKey_();
	val = GetRandomKey_();
	n_->Set(key, val);
	fields.clear();
	s_ = n_->HKeys(key, fields);
	CHECK_STATUS(OK);
	EXPECT_EQ(true, fields.empty());
	if(s_.ok() && true == fields.empty())
		log_success("key���ڣ����ǲ���hash�ṹ");
	else
		log_fail("key���ڣ����ǲ���hash�ṹ");

	fields.clear();
}

TEST_F(NemoHashTest, TestHGetall)
{
	log_message("\n========TestHGetall========");
	string key, field, val;
	vector<nemo::FV> fvs;
	
	s_.OK();//key���ڣ���fields��valֵ����
	int fieldsNum = 100, fieldsIndexStart = 0;
	key = "HGetall_test";
	log_message("======����keyΪ%s, field��%s%d��%s%d", key.c_str(), (key+"_").c_str(), fieldsIndexStart, (key+"_").c_str(), fieldsIndexStart+fieldsNum-1);
	for(int fieldsIndex = fieldsIndexStart; fieldsIndex != fieldsIndexStart+fieldsNum; fieldsIndex++)
	{
		field = key + "_" + itoa(fieldsIndex);
		n_->HSet(key, field, itoa(fieldsIndex));
	}
	s_ = n_->HGetall(key, fvs);
	CHECK_STATUS(OK);
	EXPECT_EQ(key+"_"+itoa(fieldsIndexStart), fvs.begin()->field);
	EXPECT_EQ(itoa(fieldsIndexStart), fvs.begin()->val);
	EXPECT_EQ(key+"_"+itoa(fieldsIndexStart+fieldsNum-1), fvs.rbegin()->field);
	EXPECT_EQ(itoa(fieldsIndexStart+fieldsNum-1), fvs.rbegin()->val);
	EXPECT_EQ(fieldsNum, fvs.size());
	if(s_.ok() && key+"_"+itoa(fieldsIndexStart)==fvs.begin()->field && itoa(fieldsIndexStart)==fvs.begin()->val && key+"_"+itoa(fieldsIndexStart+fieldsNum-1)==fvs.rbegin()->field && itoa(fieldsIndexStart+fieldsNum-1)==fvs.rbegin()->val && fieldsNum==fvs.size())
		log_success("key���ڣ���fields��valֵ����");
	else
		log_fail("key���ڣ���fields��valֵ����");

	s_.OK();//key���ڣ���fieled������֮���б��KV��key*
	val = GetRandomVal_();
	n_->Set(key+"1", val);
	val = GetRandomVal_();
	n_->Set(key+"2", val);
	fvs.clear();
	s_ = n_->HGetall(key, fvs);
	CHECK_STATUS(OK);
	EXPECT_EQ(key+"_"+itoa(fieldsIndexStart), fvs.begin()->field);
	EXPECT_EQ(itoa(fieldsIndexStart), fvs.begin()->val);
	EXPECT_EQ(key+"_"+itoa(fieldsIndexStart+fieldsNum-1), fvs.rbegin()->field);
	EXPECT_EQ(itoa(fieldsIndexStart+fieldsNum-1), fvs.rbegin()->val);
	EXPECT_EQ(fieldsNum, fvs.size());
	if(s_.ok() && key+"_"+itoa(fieldsIndexStart)==fvs.begin()->field && itoa(fieldsIndexStart)==fvs.begin()->val && key+"_"+itoa(fieldsIndexStart+fieldsNum-1)==fvs.rbegin()->field && itoa(fieldsIndexStart+fieldsNum-1)==fvs.rbegin()->val && fieldsNum==fvs.size())
		log_success("key���ڣ���fields��valֵ����");
	else
		log_fail("key���ڣ���fields��valֵ����");
	fvs.clear();
}

TEST_F(NemoHashTest, TestHLen)
{
	log_message("\n========TestHLen========");
	string key, field, val;
	int64_t retLen;

	s_.OK();//key������/key��field��Ӧ��ֵ������
	key = GetRandomKey_();
	field = GetRandomField_();
	retLen = n_->HLen(key);
	EXPECT_EQ(0, retLen);
	if(retLen == 0)
		log_success("key������/key��field��Ӧ��ֵ������");
	else
		log_fail("key������/key��field��Ӧ��ֵ������");

	s_.OK();//key���ڣ���fields��
	key = "HLen_Test";
	vector<string> fields;
	n_->HKeys(key, fields);
	for(vector<string>::iterator iter = fields.begin(); iter != fields.end(); iter++)
	{
		n_->HDel(key, *iter);
	}
	int fieldsNum = GetRandomUint_(0, 256);
	int fieldsIndexStart = 0;
	log_message("======����keyΪ%s, field��%s%d��%s%d", key.c_str(), (key+"_").c_str(), fieldsIndexStart, (key+"_").c_str(), fieldsIndexStart+fieldsNum-1);
	for(int fieldsIndex = fieldsIndexStart; fieldsIndex != fieldsIndexStart+fieldsNum; fieldsIndex++)
	{
		field = key + "_" + itoa(fieldsIndex);
		n_->HSet(key, field, itoa(fieldsIndex));
	}
	retLen = n_->HLen(key);
	EXPECT_EQ(fieldsNum, retLen);
	if(retLen == fieldsNum)
		log_success("key���ڣ���fields");
	else
		log_fail("key���ڣ���fields");
}

TEST_F(NemoHashTest, TestHMSet)
{
	log_message("\n========TestHMSet========");
	string key, field, val, getVal;
	vector<nemo::FV> fvs;
	bool flag;
	int fvsNum = GetRandomUint_(1, maxHMSetNum_);
	for(int index = 0; index != fvsNum; index++)
	{
		field = GetRandomField_();
		val = GetRandomVal_();
		fvs.push_back({field, val});
	}
	
	s_.OK();//KeyΪ��
	key = "";
	s_ = n_->HMSet(key, fvs);
	CHECK_STATUS(OK);
	if(s_.ok())
		log_success("KeyΪ��");
	else
		log_fail("KeyΪ��");

	s_.OK();//Key�ǿգ�FS��ֵ��������Ŀ��
	key = GetRandomKey_();
	s_ = n_->HMSet(key, fvs);
	CHECK_STATUS(OK);
	flag = true;
	for(vector<nemo::FV>::iterator iter = fvs.begin(); iter != fvs.end(); iter++)
	{
		n_->HGet(key, iter->field, &getVal);
		EXPECT_EQ(iter->val, getVal);
		if(getVal != iter->val)
		{
			flag = false;
			break;
		}
	}
	if(s_.ok() && flag)
		log_success("Key�ǿգ�FS��ֵ��������Ŀ��");
	else
		log_fail("Key�ǿգ�FS��ֵ��������Ŀ��");

	s_.OK();//Key�ǿգ�FS��ֵ�������Ŀ��
	fvs.clear();
	fvsNum = maxHMSetNum_;
	for(int index = 0; index != fvsNum; index++)
	{
		field = GetRandomField_();
		val = GetRandomVal_();
		fvs.push_back({field, val});
	}
	s_ = n_->HMSet(key, fvs);
	flag = true;
	for(vector<nemo::FV>::iterator iter = fvs.begin(); iter != fvs.end(); iter++)
	{
		n_->HGet(key, iter->field, &getVal);
		EXPECT_EQ(iter->val, getVal);
		if(iter->val != getVal)
		{
			flag = false;
			break;
		}
	}
	if(s_.ok() && flag)
		log_success("Key�ǿգ�FS��ֵ�������Ŀ��");
	else
		log_fail("Key�ǿգ�FS��ֵ�������Ŀ��");
}

TEST_F(NemoHashTest, TestHMGet)
{
	log_message("\n========TestHMGet========");
	string key, field, val;
	vector<string> fields;
	vector<nemo::FV> fvs;
	vector<nemo::FVS>fvss;
	bool flag;

	s_.OK();//Key�ǿգ�fields��ֵ(���ж�Ӧ��val)��������Ŀ��
	key = "HMGet_Test";
	int32_t hgetNum = GetRandomUint_(1, maxHMGetNum_);
	while(hgetNum>0)
	{
		field = GetRandomField_();
		val = GetRandomVal_();
		n_->HSet(key, field, val);
		fields.push_back(field);
		fvs.push_back({field, val});
		hgetNum--;
	}
	s_ = n_->HMGet(key, fields, fvss);
	CHECK_STATUS(OK);
	flag = true;
	for(vector<nemo::FVS>::iterator iter = fvss.begin(); iter != fvss.end(); iter++)
	{
		if(!((iter->status).ok()))
		{
			flag = false;
			break;
		}
	}
	EXPECT_EQ(true, flag);
	if(flag == true && s_.ok())
		log_success("Key�ǿգ�fields��ֵ(���ж�Ӧ��val)��������Ŀ��");
	else
		log_fail("Key�ǿգ�fields��ֵ(���ж�Ӧ��val)��������Ŀ��");

	s_.OK();//Key�ǿգ�fields��ֵ(��û�ж�Ӧ��val)��������Ŀ��
	int32_t arbiterNum = GetRandomUint_(0, hgetNum);
	int32_t index = 0;
	for(vector<string>::iterator iter = fields.begin(); iter != fields.end(); iter++)
	{
		if(index == arbiterNum)
		{
			n_->HDel(key, *iter);
			break;
		}
		index++;
	}
	fvss.clear();
	fvs.clear();
	s_ = n_->HMGet(key, fields, fvss);
	CHECK_STATUS(OK);
	flag = true;
	index = 0;
	for(vector<nemo::FVS>::iterator iter = fvss.begin(); iter != fvss.end(); iter++)
	{
		if(index != arbiterNum && !((iter->status).ok()))
		{
			flag = false;
			break;
		}
		if(index == arbiterNum && (iter->status).ok())
		{
			flag =  false;
			break;
		}
		index++;
	}
	EXPECT_EQ(true, flag);
	if(s_.ok() && true == flag)
		log_success("Key�ǿգ�fields��ֵ(��û�ж�Ӧ��val)��������Ŀ��");
	else
		log_fail("Key�ǿգ�fields��ֵ(��û�ж�Ӧ��val)��������Ŀ��");

	s_.OK();//Key�ǿգ�fields��ֵ(���ж�Ӧ��val)�������Ŀ��
	fields.clear();
	fvs.clear();
	fvss.clear();
	hgetNum = maxHMGetNum_;
	while(hgetNum>0)
	{
		field = GetRandomField_();
		val = GetRandomVal_();
		n_->HSet(key, field, val);
		fields.push_back(field);
		fvs.push_back({field, val});
		hgetNum--;
	}
	s_ = n_->HMGet(key, fields, fvss);
	CHECK_STATUS(OK);
	flag = true;
	for(vector<nemo::FVS>::iterator iter = fvss.begin(); iter != fvss.end(); iter++)
	{
		if(!((iter->status).ok()))
		{
			flag = false;
			break;
		}
	}
	EXPECT_EQ(true, flag);
	if(flag == true && s_.ok())
		log_success("Key�ǿգ�fields��ֵ(���ж�Ӧ��val)�������Ŀ��");
	else
		log_fail("Key�ǿգ�fields��ֵ(���ж�Ӧ��val)�������Ŀ��");	
	fields.clear();
	fvs.clear();
	fvss.clear();
}

TEST_F(NemoHashTest, TestHSetnx)
{
	log_message("\n========TestHSetnx========");

	string key, field, val;
	
	s_.OK();//key��fields��Ӧ��ԭ��ֵ�ǲ����ڵ�
	key = GetRandomKey_();
	field = GetRandomField_();
	val = GetRandomVal_();
	s_ = n_->HSetnx(key, field, val);
	CHECK_STATUS(OK);
	if(s_.ok())
		log_success("key��fields��Ӧ��ԭ��ֵ�ǲ����ڵ�");
	else
		log_fail("key��fields��Ӧ��ԭ��ֵ�ǲ����ڵ�");

	s_.OK();//key��fields��Ӧ��ԭ��ֵ�Ǵ��ڵ�
	val = GetRandomVal_();
	s_ = n_->HSetnx(key, field, val);
	CHECK_STATUS(Corruption);
	if(s_.IsCorruption())
		log_success("key��fields��Ӧ��ԭ��ֵ�Ǵ��ڵ�");
	else
		log_fail("key��fields��Ӧ��ԭ��ֵ�Ǵ��ڵ�");
}

TEST_F(NemoHashTest, TestHStrlen)
{
	log_message("\n========TestHStrlen========");
	string key, field, val;
	int64_t retValLen;

	s_.OK();//key��field��Ӧ��ֵ������
	key = GetRandomKey_();
	field = GetRandomField_();
	retValLen = n_->HStrlen(key, field);
	EXPECT_EQ(0, retValLen);
	if(0 == retValLen)
		log_success("key��field��Ӧ��ֵ������");
	else
		log_fail("key��field��Ӧ��ֵ������");

	s_.OK();//key��field��Ӧ��ֵ��val����Ϊ0
	val = "";
	n_->HSet(key, field, val);
	retValLen = n_->HStrlen(key, field);
	EXPECT_EQ(0, retValLen);
	if(0 == retValLen)
		log_success("key��field��Ӧ��ֵ��val����Ϊ0");
	else
		log_fail("key��field��Ӧ��ֵ��val����Ϊ0");

	s_.OK();//key��field��Ӧ��ֵ��val����һ��
	val = GetRandomVal_();
	n_->HSet(key, field, val);
	retValLen = n_->HStrlen(key, field);
	EXPECT_EQ(val.length(), retValLen);
	if(val.length() == retValLen)
		log_success("key��field��Ӧ��ֵ��val����һ��");
	else
		log_fail("key��field��Ӧ��ֵ��val����һ��");

	s_.OK();//key��field��Ӧ��ֵ��val�������
	val = GetRandomBytes_(maxValLen_);
	n_->HSet(key, field, val);
	retValLen = n_->HStrlen(key, field);
	//EXPECT_EQ(maxValLen_, retValLen);	
	EXPECT_EQ(val.length(), retValLen);
	if(maxValLen_ == retValLen)
		log_success("key��field��Ӧ��ֵ��val�������");
	else
		log_fail("key��field��Ӧ��ֵ��val�������");
}


TEST_F(NemoHashTest, TestHScan)
{
	
	log_message("\n========TestHScan========");
	string key, field, val, start, end;
	vector<string>fields;
	nemo::HIterator *hiter;
	key = "HScan_Test";
	int32_t count, startInt, endInt, totalFieldsNum = 100, numPre = 10000, index;
	uint64_t limit = -1;
	bool flag;
	
	n_->HKeys(key, fields);
	for(vector<string>::iterator iter = fields.begin(); iter != fields.end(); iter++)
	{
    fprintf (stderr, "hdel (%s)\n", iter->c_str());
		n_->HDel(key, *iter);
	}

	s_.OK();//key�ǿգ�start�ǿգ�end�ǿգ�����key�ϲ���hash�ṹ
	val = GetRandomVal_();
	n_->Set(key, val);
	start = "";
	end = "";
	hiter = n_->HScan(key, start, end, limit);
	count = 0;
	while(hiter->Valid())
	{
		count++;
		hiter->Next();
	}
	EXPECT_EQ(0, count);
	if(0 == count)
		log_success("key�ǿգ�start�ǿգ�end�ǿգ�����key�ϲ���hash�ṹ");
	else
		log_fail("key�ǿգ�start�ǿգ�end�ǿգ�����key�ϲ���hash�ṹ");

    int64_t del_ret;
	n_->Del(key, &del_ret);
	delete hiter;
	
	log_message("========Key is %s, field from %s_%d to %s_%d", key.c_str(), key.c_str(), numPre+0, key.c_str(), numPre+totalFieldsNum-1);
	for(int32_t index = 0; index != totalFieldsNum; index++)
	{
		n_->HSet(key, key + "_" + itoa(numPre+index), itoa(numPre+index));
	}
	
#define HScanLoopProcess(endCompare, limit, message)  \
	hiter = n_->HScan(key, start, end, limit);\
	index = startInt;\
	flag = true;\
  for (; hiter->Valid(); hiter->Next())\
	{\
		EXPECT_EQ(key + "_" + itoa(numPre + index), hiter->field());\
		if(key + "_" + itoa(numPre + index) != hiter->field())\
		{\
			flag = false;\
		}\
		index++;\
	}\
	EXPECT_EQ(endCompare, index);\
	if(flag && (endCompare == index))\
		log_success(message);\
	else\
		log_fail(message);\
	delete hiter

	s_.OK();//key�ǿգ�start�ǿգ�end�ǿգ�����key��start��end��Ӧ�����ݲ�����
	startInt = totalFieldsNum + 10;
	endInt = totalFieldsNum + 20;
	start = key + "_" + itoa(numPre+startInt);
	end = key + "_" + itoa(numPre+endInt);
	HScanLoopProcess(startInt, limit, "key�ǿգ�start�ǿգ�end�ǿգ�����key��start��end��Ӧ�����ݲ�����");




	s_.OK();//key�ǿգ�start��end�ǿգ��Ҷ���keys��Χ�ڣ����������
	startInt = GetRandomUint_(0, totalFieldsNum);
	endInt = GetRandomUint_(startInt, totalFieldsNum);
	start = key + "_" + itoa(numPre+startInt);
	end = key + "_" + itoa(numPre+endInt);
	HScanLoopProcess(endInt+1, limit, "key�ǿգ�start��end�ǿգ��Ҷ���keys��Χ�ڣ����������");


	
	s_.OK();//����limitΪ�������Ƿ���������
	startInt = GetRandomUint_(0, totalFieldsNum-10);
	endInt = GetRandomUint_(startInt+5, totalFieldsNum);
	start = key + "_" + itoa(numPre+startInt);
	end = key + "_" + itoa(numPre+endInt);
	HScanLoopProcess(startInt+3, 3, "����limitΪ�������Ƿ���������");
	

	s_.OK();//key�ǿգ�start��end�ǿգ��Ҷ���keys��Χ�ڣ�start>end��
	startInt = GetRandomUint_(1, totalFieldsNum);
	endInt = GetRandomUint_(0, startInt-1);
	start = key + "_" + itoa(numPre+startInt);
	end = key + "_" + itoa(numPre+endInt);
	HScanLoopProcess(startInt, limit, "key�ǿգ�start��end�ǿգ��Ҷ���keys��Χ�ڣ�start>end��");

	s_.OK();//key�ǿգ�start�ǿգ�endΪ��
	startInt = GetRandomUint_(0, totalFieldsNum);
	start = key + "_" + itoa(numPre+startInt);
	end = "";
	HScanLoopProcess(totalFieldsNum, limit, "key�ǿգ�start�ǿգ�endΪ��");

	s_.OK();//startΪ�գ�end��Ϊ��
	startInt = 0;
	endInt = GetRandomUint_(0, totalFieldsNum);
	start = "";
	end = key + "_" + itoa(numPre+endInt);
	HScanLoopProcess(endInt+1, limit, "startΪ�գ�end��Ϊ��");

	s_.OK();//key�ǿգ�startΪ�գ�endΪ��
	startInt = 0;
	start = "";
	end = "";
	HScanLoopProcess(totalFieldsNum, limit, "key�ǿգ�startΪ�գ�endΪ��");

	string key2, field2, val2;
	int32_t totalFieldsNum2 = 50;
	key2 = "";
	
	s_.OK();//key�գ�startΪ�գ�endΪ��
	fields.clear();
	n_->HKeys(key2, fields);
	for(vector<string>::iterator iter = fields.begin(); iter != fields.end(); iter++) {
		n_->HDel(key2, *iter);
	}
	for(int32_t index2 = 0; index2 != totalFieldsNum2; index2++)
	{
		field2 = key2 + "_" + itoa(numPre+index2);
		n_->HSet(key2, field2, itoa(numPre+index2));
	}
	hiter = n_->HScan("", "", "", -1);
	index = 0;
  for (; hiter->Valid(); hiter->Next())
	{
		index++;
	}
	EXPECT_EQ(totalFieldsNum2, index);
	if(totalFieldsNum2 == index)
		log_success("key�գ�startΪ�գ�endΪ��");
	else
		log_fail("key�գ�startΪ�գ�endΪ��");
	delete hiter;
	
}

TEST_F(NemoHashTest, TestHVals)
{
	log_message("\n========TestHVals========");
	string key, field, val;
	vector<string> vals;
	vector<string> fields;
	bool flag;

	s_.OK();//key���ڣ�����key����hash�ṹ
	key = GetRandomKey_();
	val = GetRandomVal_();
	n_->Set(key, val);
	s_ = n_->HVals(key, vals);
	CHECK_STATUS(OK);
	EXPECT_EQ(true, vals.empty());
	if(s_.ok() && vals.empty())
		log_success("key���ڣ�����key����hash�ṹ");
	else
		log_success("key���ڣ�����key����hash�ṹ");
    int64_t del_ret;
	n_->Del(key, &del_ret);

	s_.OK();//key���ڣ�û��fields
	key = GetRandomKey_();
	field = GetRandomField_();
	val = GetRandomVal_();
	n_->HSet(key, field, val);
	n_->HDel(key, field);
	val.clear();
	s_ = n_->HVals(key, vals);
	CHECK_STATUS(OK);
	EXPECT_EQ(true, vals.empty());
	if(s_.ok() && vals.empty())
		log_success("key���ڣ�û��fields");
	else
		log_fail("key���ڣ�û��fields");

	
	int32_t totalFieldsNum = 100;
	int32_t numPre = 10000;
	key = "HVals_test";
	n_->HKeys(key, fields);
	for(vector<string>::iterator iter = fields.begin(); iter != fields.end(); iter++)
	{
		n_->HDel(key, *iter);
	}
	for(int32_t index = 0; index != totalFieldsNum; index++)
	{
		field = key + "_" +	itoa(numPre+index);
		val = itoa(numPre+index);
		n_->HSet(key, field, val);
	}

	s_.OK();//key���ڣ���fields��
	vals.clear();
	s_ = n_->HVals(key, vals);
	CHECK_STATUS(OK);
	EXPECT_EQ(totalFieldsNum, vals.size());
	if(totalFieldsNum == vals.size() && s_.ok())
		log_success("key���ڣ���fields");
	else
		 log_fail("key���ڣ���fields");

	s_.OK();//keyΪ�գ�
	string key2 = "", field2, val2;
	int32_t totalFieldsNum2 = 300;
	n_->HKeys(key, fields);
	for(vector<string>::iterator iter = fields.begin(); iter != fields.end(); iter++)
	{
		n_->HDel(key2, *iter);
	}
	for(int32_t index = 0; index != totalFieldsNum2; index++)
	{
		field2 = key2 + "_" + itoa(numPre+index);
		val2 = itoa(numPre+index);
		n_->HSet(key2, field2, val2);
	}
	vals.clear();
	s_ = n_->HVals("", vals);
	CHECK_STATUS(OK);
	EXPECT_EQ(totalFieldsNum2,vals.size());
	if(s_.ok() &&  vals.size() == totalFieldsNum2)
		log_success("keyΪ��");
	else
		log_fail("keyΪ��");

	vals.clear();
	fields.clear();
}


TEST_F(NemoHashTest, TestHIncrby)
{
	log_message("\n========TestHIncrby========");
	string key, field, val, newVal;
	int64_t by;

#define HIncybyPositiveLoopProcess(result, message) \
	s_ = n_->HIncrby(key, field, by, newVal);\
	CHECK_STATUS(OK);\
	EXPECT_EQ(result, atoi(newVal.c_str()));\
	if(s_.ok() && result == atoi(newVal.c_str()))\
		log_success(message);\
	else\
		log_fail(message)

#define HIncybyNegativeLoopProcess(status, result, message) \
	s_ = n_->HIncrby(key, field, by, newVal);\
	CHECK_STATUS(status);\
	EXPECT_EQ(result, atoi(newVal.c_str()));\
	if(!(s_.ok()) && result == atoi(newVal.c_str()))\
		log_success(message);\
	else\
		log_fail(message)

	s_.OK();//key��field��Ӧ��val��������ֵ��incrby������ֵ
	key = GetRandomKey_();
	field = GetRandomField_();
	val = "13";
	n_->HSet(key, field, val);
	by = 4;
	s_ = n_->HIncrby(key, field, by, newVal);
	CHECK_STATUS(OK);
	EXPECT_EQ(17, atoi(newVal.c_str()));
	if(s_.ok() && 17 == atoi(newVal.c_str()))
		log_success("key��field��Ӧ��val��������ֵ��incrby������ֵ");
	else
		log_fail("key��field��Ӧ��val��������ֵ��incrby������ֵ");

	by = 4;
	s_.OK();//key��field��Ӧ��ֵ������
	n_->HSet(key, field, "2");
	n_->HDel(key, field);
	newVal = "0";
	HIncybyPositiveLoopProcess(by, "key��field��Ӧ��ֵ������");

	s_.OK();//key��field��Ӧ��valֻ��һ���Ӻ�
	val = "+";
	n_->HSet(key, field, val);
	newVal = "0";
	HIncybyNegativeLoopProcess(Corruption, 0, "key��field��Ӧ��valֻ��һ���Ӻ�");

	s_.OK();//key��field��Ӧ��valֻ��һ������
	val = "-";
	n_->HSet(key, field, val);
	newVal = "0";
	HIncybyNegativeLoopProcess(Corruption, 0, "key��field��Ӧ��valֻ��һ������");
	
	s_.OK();//key��field��Ӧ��val��ȫ������
	val = "123#A";
	n_->HSet(key, field, val);
	newVal = "0";
	HIncybyNegativeLoopProcess(Corruption, 0, "key��field��Ӧ��val��ȫ������");

	s_.OK();//key��field��ӦvalΪ��0000��
	val = "00000";
	n_->HSet(key, field, val);
	newVal = "0";
	by = 5;
	HIncybyPositiveLoopProcess(5, "key��field��ӦvalΪ��0000��");

	s_.OK();//key��field��Ӧ��val����+�ţ��磺��+10��
	val = "+10";
	n_->HSet(key, field, val);
	newVal = "0";
	by = 5;
	HIncybyPositiveLoopProcess(15, "key��field��Ӧ��val����+�ţ��磺��+10��");

	s_.OK();//key��field��Ӧ��val����+�ţ��磺��-10��
	val = "-10";
	n_->HSet(key, field, val);
	newVal = "0";
	by = 5;
	HIncybyPositiveLoopProcess(-5, "key��field��Ӧ��val����+�ţ��磺��-10");

	s_.OK();//�������/С���������
	val = to_string(LLONG_MAX);
	n_->HSet(key, field, val);
	newVal = "0";
	by = 5;
	HIncybyNegativeLoopProcess(Invalid, 0, "�������/С���������");
}

TEST_F(NemoHashTest, TestHIncrbyfloat)
{
	log_message("\n========TestHIncrbyfloat========");
	string key, field, val, newVal;
	double by, diff;

	s_.OK();//key��field��Ӧ������������ֵ��floatby��������ֵ
	key = GetRandomKey_();
	field = GetRandomField_();
	val = "12.3";
	by = 1.2;
	n_->HSet(key, field, val);
	s_ = n_->HIncrbyfloat(key, field, by, newVal);
	CHECK_STATUS(OK);
	diff = atof(newVal.c_str()) - atof(val.c_str()) - by;
	EXPECT_EQ(true, diff < eps && diff > -eps);
	if(s_.ok() && diff < eps && diff > -eps)
		log_success("key��field��Ӧ������������ֵ��floatby��������ֵ");
	else
		log_fail("key��field��Ӧ������������ֵ��floatby��������ֵ");

	s_.OK();//key��field��Ӧ��ֵ������
	n_->HDel(key, field);
	s_ = n_->HIncrbyfloat(key, field, by, newVal);
	CHECK_STATUS(OK);
	diff = atof(newVal.c_str()) - by;
	EXPECT_EQ(true, diff < eps && diff > -eps);
	if(s_.ok() && diff < eps && diff > -eps)
		log_success("key��field��Ӧ��ֵ������");
	else
		log_fail("key��field��Ӧ��ֵ������");

	s_.OK();//key��field��Ӧ��ԭval�з������ַ�
	val = "1.23jfkdj";
	newVal = "0.0";
	n_->HSet(key, field, val);
	s_ = n_->HIncrbyfloat(key, field, by, newVal);
	CHECK_STATUS(Corruption);
	EXPECT_EQ(string("0.0"), newVal);
	if(s_.IsCorruption() && string("0.0") == newVal)
		log_success("key��field��Ӧ��ԭval�з������ַ�");
	else
		log_fail("key��field��Ӧ��ԭval�з������ַ�");

	s_.OK();//key��field��Ӧ��ֵֻ��һ��+
	val = "+";
	newVal = "0.0";
	n_->HSet(key, field, val);
	s_ = n_->HIncrbyfloat(key, field, by, newVal);
	CHECK_STATUS(Corruption);
	EXPECT_EQ(string("0.0"), newVal);
	if(s_.IsCorruption() && string("0.0") == newVal)
		log_success("key��field��Ӧ��ֵֻ��һ��+");
	else
		log_fail("key��field��Ӧ��ֵֻ��һ��+");

	s_.OK();//key��field��Ӧ��ֵֻ��һ��-
	val = "-";
	newVal = "0.0";
	n_->HSet(key, field, val);
	s_ = n_->HIncrbyfloat(key, field, by, newVal);
	CHECK_STATUS(Corruption);
	EXPECT_EQ(string("0.0"), newVal);
	if(s_.IsCorruption() && string("0.0") == newVal)
		log_success("key��field��Ӧ��ֵֻ��һ��-");
	else
		log_fail("key��field��Ӧ��ֵֻ��һ��-");

	s_.OK();//key��field��Ӧ��ֵ�ԡ�+����ͷ
	val = "+11.3";
	by = 23.4;
	newVal = "0.0";
	n_->HSet(key, field, val);
	s_ = n_->HIncrbyfloat(key, field, by, newVal);
	CHECK_STATUS(OK);
	diff = atof(newVal.c_str()) - atof(val.c_str()) - by;
	EXPECT_EQ(true, diff < eps && diff > -eps);
	if(s_.ok() && diff < eps && diff > -eps)
		log_success("key��field��Ӧ��ֵ��+��ͷ");
	else
		log_fail("key��field��Ӧ��ֵ��+��ͷ");

	s_.OK();//key��field��Ӧ��ֵ�ԡ�+����ͷ
	val = "-13.4";
	by = 22.34;
	newVal = "0.0";
	n_->HSet(key, field, val);
	s_ = n_->HIncrbyfloat(key, field, by, newVal);
	CHECK_STATUS(OK);
	diff = atof(newVal.c_str()) - atof(val.c_str()) - by;
	EXPECT_EQ(true, diff < eps && diff > -eps);
	if(s_.ok() && diff < eps && diff > -eps)
		log_success("key��field��Ӧ��ֵ��-��ͷ");
	else
		log_fail("key��field��Ӧ��ֵ��-��ͷ");

	s_.OK();//���ӵĽ����С��λ����0
	val = "3.34";
	by = 7.66;
	newVal = "0.0";
	n_->HSet(key, field, val);
	s_ = n_->HIncrbyfloat(key, field, by, newVal);
	CHECK_STATUS(OK);
	EXPECT_EQ(string("11"), newVal);
	if(s_.ok() && string("11") == newVal)
		log_success("���ӵĽ����С��λ����0");
	else
		log_fail("���ӵĽ����С��λ����0");

	log_message("============================HASHTEST END===========================");
	log_message("============================HASHTEST END===========================\n\n");
}
