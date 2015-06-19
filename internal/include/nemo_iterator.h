#ifndef NEMO_INCLUDE_NEMO_ITERATOR_H_
#define NEMO_INCLUDE_NEMO_ITERATOR_H_
#include "rocksdb/db.h"
#include "nemo_const.h"

namespace nemo {

enum Direction {
    kForward = 0,
    kBackward = 1
};

class Iterator {
public:
    Iterator(rocksdb::Iterator *it, const std::string &end, uint64_t limit, Direction direction = kForward);
    ~Iterator();
    bool Skip(uint64_t offset);
    bool Next();
    rocksdb::Slice Key();
    rocksdb::Slice Val();
private:
    rocksdb::Iterator *it_;
    std::string end_;
    uint64_t limit_;
    bool is_first_;
    int direction_;
    //No Copying Allowed
    Iterator(Iterator&);
    void operator=(Iterator&);
};

class KIterator {
public:
    KIterator(Iterator *it);
    ~KIterator();
    bool Next();
    std::string Key() { return key_; };
    std::string Val() { return val_; };
private:
    Iterator *it_;
    std::string key_;
    std::string val_;
    //No Copying Allowed
    KIterator(KIterator&);
    void operator=(KIterator&);
};

class HIterator {
public:
    HIterator(Iterator *it, const rocksdb::Slice &key);
    ~HIterator();
    bool Next();
    std::string Key() { return key_; };
    std::string Field() { return field_; };
    std::string Val() { return val_; };
private:
    Iterator *it_;
    std::string key_;
    std::string field_;
    std::string val_;
    //No Copying Allowed
    HIterator(HIterator&);
    void operator=(HIterator&);
};

class ZIterator {
public:
    ZIterator(Iterator *it, const rocksdb::Slice &key);
    ~ZIterator();
    bool Next();
    std::string Key() { return key_; };
    int64_t Score() { return score_; };
    std::string Member() { return member_; };
private:
    Iterator *it_;
    std::string key_;
    int64_t score_;
    std::string member_;
    //No Copying Allowed
    ZIterator(ZIterator&);
    void operator=(ZIterator&);
    
};
}
#endif
