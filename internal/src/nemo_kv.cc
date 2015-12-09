#include <ctime>
#include <climits>

#include "nemo.h"
#include "nemo_iterator.h"
#include "util.h"
#include "xdebug.h"

using namespace nemo;

Status Nemo::Set(const std::string &key, const std::string &val, const int32_t ttl) {
    Status s;
    if (ttl <= 0) {
        s = kv_db_->Put(rocksdb::WriteOptions(), key, val);
    } else {
        s = kv_db_->PutWithKeyTTL(rocksdb::WriteOptions(), key, val, ttl);
    }
    return s;
}

Status Nemo::Get(const std::string &key, std::string *val) {
    Status s;
    s = kv_db_->Get(rocksdb::ReadOptions(), key, val);
    return s;
}

Status Nemo::KDel(const std::string &key) {
    Status s;
    s = kv_db_->Delete(rocksdb::WriteOptions(), key);
    return s;
}


Status Nemo::MSet(const std::vector<KV> &kvs) {
    Status s;
    std::vector<KV>::const_iterator it;
    rocksdb::WriteBatch batch;
    for (it = kvs.begin(); it != kvs.end(); it++) {
        batch.Put(it->key, it->val); 
    }
    s = kv_db_->WriteWithKeyTTL(rocksdb::WriteOptions(), &(batch), 0);
    return s;
}

Status Nemo::MDel(const std::vector<std::string> &keys, int64_t* count) {
    *count = 0;
    Status s;
    std::string val;
    std::vector<std::string>::const_iterator it;
    rocksdb::WriteBatch batch;
    for (it = keys.begin(); it != keys.end(); it++) {
        s = kv_db_->Get(rocksdb::ReadOptions(), *it, &val);
        if (s.ok()) {
            (*count)++;
            batch.Delete(*it); 
        }
    }
    s = kv_db_->Write(rocksdb::WriteOptions(), &(batch));
    return s;

}

Status Nemo::MGet(const std::vector<std::string> &keys, std::vector<KVS> &kvss) {
    Status s;
    std::vector<std::string>::const_iterator it_key;
    for (it_key = keys.begin(); it_key != keys.end(); it_key++) {
        std::string val("");
        s = kv_db_->Get(rocksdb::ReadOptions(), *it_key, &val);
        kvss.push_back((KVS){*(it_key), val, s});
    }
    return Status::OK();
}

Status Nemo::Incrby(const std::string &key, const int64_t by, std::string &new_val) {
    Status s;
    std::string val;
    MutexLock l(&mutex_kv_);
    s = kv_db_->Get(rocksdb::ReadOptions(), key, &val);
    if (s.IsNotFound()) {
        new_val = std::to_string(by);        
    } else if (s.ok()) {
        int64_t ival;
        if (!StrToInt64(val.data(), val.size(), &ival)) {
            return Status::Corruption("value is not a integer");
        }
        if ((by >= 0 && LLONG_MAX - by < ival) || (by < 0 && LLONG_MIN - by > ival)) {
            return Status::InvalidArgument("Overflow");
        }
        new_val = std::to_string(ival + by);
    } else {
        return Status::Corruption("Get error");
    }
    int64_t ttl;
    s = TTL(key, &ttl);
    if (ttl) {
        s = kv_db_->PutWithKeyTTL(rocksdb::WriteOptions(), key, new_val, (int32_t)ttl);
    } else {
        s = kv_db_->Put(rocksdb::WriteOptions(), key, new_val);
    }
    return s;
}

Status Nemo::Decrby(const std::string &key, const int64_t by, std::string &new_val) {
    Status s;
    std::string val;
    MutexLock l(&mutex_kv_);
    s = kv_db_->Get(rocksdb::ReadOptions(), key, &val);
    if (s.IsNotFound()) {
        new_val = std::to_string(-by);        
    } else if (s.ok()) {
        int64_t ival;
        if (!StrToInt64(val.data(), val.size(), &ival)) {
            return Status::Corruption("value is not a integer");
        }
        if ((by >= 0 && LLONG_MIN + by > ival) || (by < 0 && LLONG_MAX + by < ival)) {
            return Status::InvalidArgument("Overflow");
        }
        new_val = std::to_string(ival - by);
    } else {
        return Status::Corruption("Get error");
    }
    int64_t ttl;
    s = TTL(key, &ttl);
    if (ttl) {
        s = kv_db_->PutWithKeyTTL(rocksdb::WriteOptions(), key, new_val, (int32_t)ttl);
    } else {
        s = kv_db_->Put(rocksdb::WriteOptions(), key, new_val);
    }
    return s;
}

Status Nemo::Incrbyfloat(const std::string &key, const double by, std::string &new_val) {
    Status s;
    std::string val;
    std::string res;
    MutexLock l(&mutex_kv_);
    s = kv_db_->Get(rocksdb::ReadOptions(), key, &val);
    if (s.IsNotFound()) {
        new_val = std::to_string(-by);        
    } else if (s.ok()) {
        double dval;
        if (!StrToDouble(val.data(), val.size(), &dval)) {
            return Status::Corruption("value is not a float");
        } 

        dval += by;
        if (isnan(dval) || isinf(dval)) {
            return Status::InvalidArgument("Overflow");
        }
        res = std::to_string(dval);
    } else {
        return Status::Corruption("Get error");
    }
    size_t pos = res.find_last_not_of("0", res.size());
    pos = pos == std::string::npos ? pos : pos+1;
    new_val = res.substr(0, pos); 
    if (new_val[new_val.size()-1] == '.') {
        new_val = new_val.substr(0, new_val.size()-1);
    }
    int64_t ttl;
    s = TTL(key, &ttl);
    if (ttl) {
        s = kv_db_->PutWithKeyTTL(rocksdb::WriteOptions(), key, new_val, (int32_t)ttl);
    } else {
        s = kv_db_->Put(rocksdb::WriteOptions(), key, new_val);
    }
    return s;
}

Status Nemo::GetSet(const std::string &key, const std::string &new_val, std::string *old_val) {
    Status s;
    std::string val;
    *old_val = "";
    MutexLock l(&mutex_kv_);
    s = kv_db_->Get(rocksdb::ReadOptions(), key, old_val);
    if (!s.ok() && !s.IsNotFound()) {
        return Status::Corruption("Get error");
    }else {
        s = kv_db_->Put(rocksdb::WriteOptions(), key, new_val);
        return s;
    }
}

Status Nemo::Append(const std::string &key, const std::string &value, int64_t *new_len) {
    Status s;
    *new_len = 0;
    std::string old_val;
    MutexLock l(&mutex_kv_);
    s = kv_db_->Get(rocksdb::ReadOptions(), key, &old_val);
    std::string new_val;
    if (s.ok()) {
        new_val = old_val.append(value);
    } else if (s.IsNotFound()) {
        new_val = value;
    } else {
        return s;
    }

    int64_t ttl;
    s = TTL(key, &ttl);
    if (ttl) {
        s = kv_db_->PutWithKeyTTL(rocksdb::WriteOptions(), key, new_val, (int32_t)ttl);
    } else {
        s = kv_db_->Put(rocksdb::WriteOptions(), key, new_val);
    }
    return s;
}

Status Nemo::Setnx(const std::string &key, const std::string &value, int64_t *ret, const int32_t ttl) {
    *ret = 0;
    std::string val;
    MutexLock l(&mutex_kv_);
    Status s = kv_db_->Get(rocksdb::ReadOptions(), key, &val);
    if (s.IsNotFound()) {
        if (ttl <= 0) {
            s = kv_db_->Put(rocksdb::WriteOptions(), key, value);
        } else {
            s = kv_db_->PutWithKeyTTL(rocksdb::WriteOptions(), key, value, ttl);
        }
        *ret = 1;
    }
    return s;
}

Status Nemo::Setxx(const std::string &key, const std::string &value, int64_t *ret, const int32_t ttl) {
    *ret = 0;
    std::string val;
    MutexLock l(&mutex_kv_);
    Status s = kv_db_->Get(rocksdb::ReadOptions(), key, &val);
    if (s.ok()) {
        if (ttl <= 0) {
            s = kv_db_->Put(rocksdb::WriteOptions(), key, value);
        } else {
            s = kv_db_->PutWithKeyTTL(rocksdb::WriteOptions(), key, value, ttl);
        }
        *ret = 1;
    }
    return s;
}

Status Nemo::MSetnx(const std::vector<KV> &kvs, int64_t *ret) {
    Status s;
    std::vector<KV>::const_iterator it;
    rocksdb::WriteBatch batch;
    std::string val;
    *ret = 1;
    for (it = kvs.begin(); it != kvs.end(); it++) {
        s = kv_db_->Get(rocksdb::ReadOptions(), it->key, &val);
        if (s.ok()) {
            *ret = 0;
            break;
        }
        batch.Put(it->key, it->val); 
    }
    if (*ret == 1) {
        s = kv_db_->WriteWithKeyTTL(rocksdb::WriteOptions(), &(batch), 0);
    }
    return s;
}

Status Nemo::Getrange(const std::string key, const int64_t start, const int64_t end, std::string &substr) {
    substr = "";
    std::string val;
    Status s = kv_db_->Get(rocksdb::ReadOptions(), key, &val);
    if (s.ok()) {
        int64_t size = val.length();
        int64_t start_t = start >= 0 ? start : size + start;
        int64_t end_t = end >= 0 ? end : size + end;
        if (start_t > size - 1 || (start_t != 0 && start_t > end_t) || (start_t != 0 && end_t < 0)) {
            return Status::OK();
        }
        if (start_t < 0) {
            start_t  = 0;
        }
        if (end_t >= size) {
            end_t = size - 1;
        }
        if (start_t == 0 && end_t < 0) {
            end_t = 0;
        }
        substr = val.substr(start_t, end_t-start_t+1);
    }
    return s;
}

Status Nemo::Setrange(const std::string key, const int64_t offset, const std::string &value, int64_t *len) {
    std::string val;
    std::string new_val;
    if (offset < 0) {
        return Status::Corruption("offset < 0");
    }
    MutexLock l(&mutex_kv_);
    Status s = kv_db_->Get(rocksdb::ReadOptions(), key, &val);
    if (s.ok()) {
        if (val.length() + offset > (1<<29)) {
            return Status::Corruption("too big");
        }
        if ((size_t)offset > val.length()) {
            val.resize(offset);
            new_val = val.append(value);
        } else {
            std::string head = val.substr(0, offset);
            std::string tail;
            if (offset + value.length() - 1 < val.length() -1 ) {
                tail = val.substr(offset+value.length());
            }
            new_val = head + value + tail;
        }
        *len = new_val.length();
    } else if (s.IsNotFound()) {
        std::string tmp(offset, '\0');
        new_val = tmp.append(value);
        *len = new_val.length();
    }
    int64_t ttl;
    s = TTL(key, &ttl);
    if (ttl) {
        s = kv_db_->PutWithKeyTTL(rocksdb::WriteOptions(), key, new_val, (int32_t)ttl);
    } else {
        s = kv_db_->Put(rocksdb::WriteOptions(), key, new_val);
    }
    return s;
}

Status Nemo::Strlen(const std::string &key, int64_t *len) {
    Status s;
    std::string val;
    s = Get(key, &val);
    if (s.ok()) {
        *len = val.length();
    } else if (s.IsNotFound()) {
        *len = 0;
    }
    return s;
}

KIterator* Nemo::Scan(const std::string &start, const std::string &end, uint64_t limit, bool use_snapshot) {
    std::string key_end;
    if (end.empty()) {
        key_end = "";
    } else {
        key_end = end;
    }
    rocksdb::Iterator *it;
    rocksdb::ReadOptions iterate_options;
    if (use_snapshot) {
        iterate_options.snapshot = kv_db_->GetSnapshot();
    }
    iterate_options.fill_cache = false;
    it = kv_db_->NewIterator(iterate_options);
    it->Seek(start);
    return new KIterator(new Iterator(it, key_end, limit, iterate_options)); 
}

Status Nemo::KExpire(const std::string &key, const int32_t seconds, int64_t *res) {
    Status s;
    std::string val;

    s = kv_db_->Get(rocksdb::ReadOptions(), key, &val);
    if (s.IsNotFound()) {
        *res = 0;
    } else if (s.ok()) {
        if (seconds > 0) {
            s = kv_db_->PutWithKeyTTL(rocksdb::WriteOptions(), key, val, seconds);
        } else { 
            s = kv_db_->Delete(rocksdb::WriteOptions(), key);
        }
        *res = 1;
    }
    return s;
}

Status Nemo::KTTL(const std::string &key, int64_t *res) {
    Status s;
    std::string val;

    s = kv_db_->Get(rocksdb::ReadOptions(), key, &val);
    if (s.IsNotFound()) {
        *res = -2;
    } else if (s.ok()) {
        int32_t ttl;
        s = kv_db_->GetKeyTTL(rocksdb::ReadOptions(), key, &ttl);
        *res = ttl;
    }
    return s;
}

Status Nemo::Persist(const std::string &key, int64_t *res) {
    Status s;
    std::string val;

    *res = 0;
    s = kv_db_->Get(rocksdb::ReadOptions(), key, &val);
    if (s.ok()) {
        int32_t ttl;
        s = kv_db_->GetKeyTTL(rocksdb::ReadOptions(), key, &ttl);
        if (ttl >= 0) {
            s = kv_db_->Put(rocksdb::WriteOptions(), key, val);
            *res = 1;
        }
    }
    return s;
}

Status Nemo::Expireat(const std::string &key, const int32_t timestamp, int64_t *res) {
    Status s;
    std::string val;

    s = kv_db_->Get(rocksdb::ReadOptions(), key, &val);
    if (s.IsNotFound()) {
        *res = 0;
    } else if (s.ok()) {
        std::time_t cur = std::time(0);
        if (timestamp <= cur) {
            s = kv_db_->Delete(rocksdb::WriteOptions(), key);
        } else {
            s = kv_db_->PutWithExpiredTime(rocksdb::WriteOptions(), key, val, timestamp);
        }
        *res = 1;
    }
    return s;
}

// we don't check timestamp here
Status Nemo::SetWithExpireAt(const std::string &key, const std::string &val, const int32_t timestamp) {
    //std::time_t cur = std::time(0);
    Status s;
    if (timestamp <= 0) {
        s = kv_db_->Put(rocksdb::WriteOptions(), key, val);
    } else {
        s = kv_db_->PutWithExpiredTime(rocksdb::WriteOptions(), key, val, timestamp);
    }
    return s;
}

Status Nemo::GetSnapshot(Snapshots &snapshots) {
    const rocksdb::Snapshot* psnap;

    psnap = kv_db_->GetSnapshot();
    if (psnap == nullptr) {
      return Status::Corruption("GetSnapshot failed");
    }
    snapshots.push_back(psnap);

    psnap = hash_db_->GetSnapshot();
    if (psnap == nullptr) {
      return Status::Corruption("GetSnapshot failed");
    }
    snapshots.push_back(psnap);

    psnap = zset_db_->GetSnapshot();
    if (psnap == nullptr) {
      return Status::Corruption("GetSnapshot failed");
    }
    snapshots.push_back(psnap);

    psnap = set_db_->GetSnapshot();
    if (psnap == nullptr) {
      return Status::Corruption("GetSnapshot failed");
    }
    snapshots.push_back(psnap);

    psnap = list_db_->GetSnapshot();
    if (psnap == nullptr) {
      return Status::Corruption("GetSnapshot failed");
    }
    snapshots.push_back(psnap);

    return Status::OK();
}

Status Nemo::ScanKeysWithTTL(std::unique_ptr<rocksdb::DBWithTTL> &db, Snapshot *snapshot, const std::string pattern, std::vector<std::string>& keys) {
    rocksdb::ReadOptions iterate_options;

    iterate_options.snapshot = snapshot;
    iterate_options.fill_cache = false;

    rocksdb::Iterator *it = db->NewIterator(iterate_options);

    for (it->SeekToFirst(); it->Valid(); it->Next()) {
      std::string key = it->key().ToString();
      if (stringmatchlen(pattern.data(), pattern.size(), key.data(), key.size(), 0)) {
          keys.push_back(key);
      }
       //printf ("ScanDB key=(%s) value=(%s) val_size=%u num=%lu\n", it->key().ToString().c_str(), it->value().ToString().c_str(),
       //       it->value().ToString().size(), num);
    }

    db->ReleaseSnapshot(iterate_options.snapshot);
    delete it;

    return Status::OK();
}

Status Nemo::ScanKeys(std::unique_ptr<rocksdb::DBWithTTL> &db, Snapshot *snapshot, const char kType, const std::string &pattern, std::vector<std::string>& keys) {
    rocksdb::ReadOptions iterate_options;

    iterate_options.snapshot = snapshot;
    iterate_options.fill_cache = false;

    rocksdb::Iterator *it = db->NewIterator(iterate_options);

    std::string key_start = "a";
    key_start[0] = kType;
    it->Seek(key_start);

    for (; it->Valid(); it->Next()) {
      if (kType != it->key().ToString().at(0)) {
        break;
      }
      std::string key = it->key().ToString().substr(1);
      if (stringmatchlen(pattern.data(), pattern.size(), key.data(), key.size(), 0)) {
          keys.push_back(key);
      }
       //printf ("ScanDB key=(%s) value=(%s) val_size=%u num=%lu\n", it->key().ToString().c_str(), it->value().ToString().c_str(),
       //       it->value().ToString().size(), num);
    }

    db->ReleaseSnapshot(iterate_options.snapshot);
    delete it;

    return Status::OK();
}

// String APIs

Status Nemo::Keys(const std::string &pattern, std::vector<std::string>& keys) {
    Status s;
    std::vector<const rocksdb::Snapshot*> snapshots;

    s = GetSnapshot(snapshots);
    if (!s.ok()) return s;

    s = ScanKeysWithTTL(kv_db_, snapshots[0], pattern, keys);
    if (!s.ok()) return s;

    s = ScanKeys(hash_db_, snapshots[1], DataType::kHSize, pattern, keys);
    if (!s.ok()) return s;

    s = ScanKeys(zset_db_, snapshots[2], DataType::kZSize, pattern, keys);
    if (!s.ok()) return s;

    s = ScanKeys(set_db_, snapshots[3], DataType::kSSize, pattern, keys);
    if (!s.ok()) return s;

    s = ScanKeys(list_db_, snapshots[4], DataType::kLMeta, pattern, keys);
    if (!s.ok()) return s;

}

Status Nemo::Del(const std::string &key) {
    int cnt = 0;
    Status s;
    
    s = KDel(key);
    if (s.ok()) { cnt++; }
    else if (!s.IsNotFound()) { return s; }

    s = HDelKey(key);
    if (s.ok()) { cnt++; }
    else if (!s.IsNotFound()) { return s; }

    s = ZDelKey(key);
    if (s.ok()) { cnt++; }
    else if (!s.IsNotFound()) { return s; }

    s = SDelKey(key);
    if (s.ok()) { cnt++; }
    else if (!s.IsNotFound()) { return s; }

    s = LDelKey(key);
    if (s.ok()) { cnt++; }
    else if (!s.IsNotFound()) { return s; }

    return Status::OK();
}

Status Nemo::Expire(const std::string &key, const int32_t seconds, int64_t *res) {
    int cnt = 0;
    Status s;
    
    s = KExpire(key, seconds, res);
    if (s.ok()) { cnt++; }
    else if (!s.IsNotFound()) { return s; }

    s = HExpire(key, seconds, res);
    if (s.ok()) { cnt++; }
    else if (!s.IsNotFound()) { return s; }

    s = ZExpire(key, seconds, res);
    if (s.ok()) { cnt++; }
    else if (!s.IsNotFound()) { return s; }

    s = SExpire(key, seconds, res);
    if (s.ok()) { cnt++; }
    else if (!s.IsNotFound()) { return s; }

    s = LExpire(key, seconds, res);
    if (s.ok()) { cnt++; }
    else if (!s.IsNotFound()) { return s; }

    return Status::OK();
}

Status Nemo::TTL(const std::string &key, int64_t *res) {
    Status s;
    
    s = KTTL(key, res);
    if (s.ok()) return s;

    s = HTTL(key, res);
    if (s.ok()) return s;

    s = ZTTL(key, res);
    if (s.ok()) return s;

    s = STTL(key, res);
    if (s.ok()) return s;

    s = LTTL(key, res);
    if (s.ok()) return s;

    return s; 
}
