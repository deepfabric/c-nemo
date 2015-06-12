#include "nemo.h"
#include "nemo_list.h"
#include "utilities/strings.h"
#include "xdebug.h"
using namespace nemo;

#define INIT_INDEX (INT_MAX / 2);

static int32_t ParseMeta(std::string &meta, uint64_t *len, uint64_t *left, uint64_t *right) {
    if (meta.size() != sizeof(uint64_t) * 3) {
        return -1;
    }
    *len = *((uint64_t *)(meta.data()));
    *left = *((uint64_t *)(meta.data() + sizeof(uint64_t)));
    *right = *((uint64_t *)(meta.data() + sizeof(uint64_t) * 2));
    return 0;
}

uint64_t Nemo::LLen(const std::string &key) {
    rocksdb::Status s;
    std::string meta_key = EncodeLMetaKey(key);
    std::string meta;
    uint64_t ret;
    s = db_->Get(rocksdb::ReadOptions(), meta_key, &meta);
    if (s.ok()) {
        if(meta.size() != sizeof(uint64_t) * 3) {
            return -2;
        }
        ret = *((uint64_t *)(meta.data()));
        return ret;
    } else if (s.IsNotFound()) {
        return 0;
    } else {
        return -1;
    }
}

rocksdb::Status Nemo::LPush(const std::string &key, const std::string &val) {
    rocksdb::Status s;
    rocksdb::WriteBatch batch;
    std::string meta;
    uint64_t len;
    uint64_t left;
    uint64_t right;
    std::string meta_key = EncodeLMetaKey(key);
    MutexLock l(&mutex_list_);
    s = db_->Get(rocksdb::ReadOptions(), meta_key, &meta);
    if (s.ok()) {
        if (ParseMeta(meta, &len, &left, &right) == 0) {
            if (len == INT_MAX - 2) {
                return rocksdb::Status::Corruption("list reach max length");
            }
            if (left == 0) {
                return rocksdb::Status::Corruption("list left out of range");
            }
            std::string db_key = EncodeListKey(key, Uint64ToStr(left));
            batch.Put(db_key, val);
            len++;
            left--;
            *((uint64_t *)meta.data()) = len;
            *((uint64_t *)(meta.data() + sizeof(uint64_t))) = left;
            batch.Put(meta_key, meta);
            s = db_->Write(rocksdb::WriteOptions(), &batch);
            return s;
        } else {
            return rocksdb::Status::Corruption("parse listmeta error");
        }
    } else if (s.IsNotFound()) {
        len = 1;
        left = INIT_INDEX;
        right = left + 1;
        uint64_t meta[3];
        meta[0] = len;
        meta[1] = left - 1;
        meta[2] = right;
        std::string meta_str((char *)meta, 3 * sizeof(uint64_t));
        batch.Put(meta_key, meta_str);
        batch.Put(EncodeListKey(key, Uint64ToStr(left)), val);
        s = db_->Write(rocksdb::WriteOptions(), &batch);
        return s;
    } else {
        return rocksdb::Status::Corruption("get listmeta error");
    }
}

rocksdb::Status Nemo::LPop(const std::string &key, std::string *val) {
    rocksdb::Status s;
    rocksdb::WriteBatch batch;
    std::string meta;
    uint64_t len;
    uint64_t left;
    uint64_t right;
    std::string meta_key = EncodeLMetaKey(key);
    MutexLock l(&mutex_list_);
    s = db_->Get(rocksdb::ReadOptions(), meta_key, &meta);
    if (s.ok()) {
        if (ParseMeta(meta, &len, &left, &right) == 0) {
            if (len == 0 || left + 1 == INT_MAX) {
                s = db_->Delete(rocksdb::WriteOptions(), meta_key);
                return rocksdb::Status::NotFound("not found key");
            }
            if (--len == 0) {
                batch.Delete(meta_key);
            }
            left++;
            uint64_t meta[3];
            meta[0] = len;
            meta[1] = left - 1;
            meta[2] = right;
            std::string meta_str((char *)meta, 3 * sizeof(uint64_t));
            batch.Put(meta_key, meta_str);
            std::string db_key = EncodeListKey(key, Uint64ToStr(left));
            s = db_->Get(rocksdb::ReadOptions(), db_key, val);
            batch.Delete(db_key);
            s = db_->Write(rocksdb::WriteOptions(), &batch);
            return s;
        }
    } else if (s.IsNotFound()) {
        return rocksdb::Status::NotFound("not found key");
    } else {
        return rocksdb::Status::Corruption("get listmeta error");
    }
}
