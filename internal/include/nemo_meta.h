#ifndef NEMO_INCLUDE_META_H_
#define NEMO_INCLUDE_META_H_

#include <memory>
#include "nemo_const.h"
#include "util.h"

namespace nemo {

class NemoMeta;
typedef std::shared_ptr<NemoMeta> MetaPtr;
class NemoMeta {
public:
  virtual ~NemoMeta() {}
  // Construct NemoMeta from string
  virtual bool DecodeFrom(const std::string& raw_meta) = 0;
  // Encode MemoMeta to string
  virtual bool EncodeTo(std::string& meta) = 0;
  virtual std::string ToString() = 0;
  virtual int64_t Volume() = 0;
  virtual int64_t Length() = 0;
  static bool Create(DBType type, MetaPtr &p_meta);
};

struct DefaultMeta : public NemoMeta {
  int64_t len;
  int64_t vol;

  DefaultMeta() : len(0),vol(0) {}
  explicit DefaultMeta(int64_t _len,int64_t _vol):len(_len),vol(_vol) {}
  virtual bool DecodeFrom(const std::string& raw_meta) {
    if (raw_meta.size() != sizeof(int64_t)+sizeof(int64_t)) {
      return false;
    }
    len = *(int64_t *)raw_meta.data();
    vol = *(int64_t *)(raw_meta.data()+sizeof(int64_t));
    return true;
  }
  virtual bool EncodeTo(std::string& raw_meta) {
    raw_meta.clear();
    raw_meta.append((char *)&len, sizeof(int64_t));
    raw_meta.append((char *)&vol, sizeof(int64_t));
    return true;
  }
  virtual std::string ToString() {
    char buf[32];
    std::string res("Len : ");
    Int64ToStr(buf, 32, len);
    res.append(buf);
    res.append(";Vol : ");
    Int64ToStr(buf, 32, vol);
    res.append(buf);
    return res;
  }
  virtual int64_t Volume() {
    return vol;
  }
  virtual int64_t Length() {
    return len;
  }
};

//typedef DefaultMeta HashMeta;
typedef DefaultMeta SetMeta;
typedef DefaultMeta ZSetMeta;

struct HashMeta : public NemoMeta {
  int64_t len;
  int64_t vol;
  int64_t index_len;
  const char *  index;

  HashMeta() : len(0), vol(0),index_len(0),index(nullptr)  {}
  HashMeta(int64_t _len, int64_t _vol, int64_t _index_len, char * _index)
      : len(_len), vol(_vol), index_len(_index_len),index(_index) {}
  virtual bool DecodeFrom(const std::string& raw_meta);
  virtual bool EncodeTo(std::string& raw_meta);
  virtual std::string ToString() {
    char buf[32];
    std::string res("Len : ");
    Int64ToStr(buf, 32, len);
    res.append(buf);
    res.append(";Vol : ");
    Int64ToStr(buf, 32, vol);
    res.append(buf);
    return res;
  }  
  virtual int64_t Volume() {
    return vol;
  }
  virtual int64_t Length() {
    return len;
  }
};

struct ListMeta : public NemoMeta {
  int64_t len;
  int64_t vol;
  int64_t left;
  int64_t right;
  int64_t cur_seq;

  ListMeta() : len(0), vol(0), left(0), right(0), cur_seq(1) {}
  ListMeta(int64_t _len, int64_t _vol, int64_t _left, int64_t _right, int64_t cseq)
      : len(_len), vol(_vol), left(_left), right(_right), cur_seq(cseq) {}
  virtual bool DecodeFrom(const std::string& raw_meta);
  virtual bool EncodeTo(std::string& raw_meta);
  virtual std::string ToString();
  virtual int64_t Volume() {
    return vol;
  }
  virtual int64_t Length() {
    return len;
  }
};

}
#endif
