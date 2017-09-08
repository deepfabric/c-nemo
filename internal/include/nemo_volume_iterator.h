#ifndef NEMO_INCLUDE_NEMO_VOLUME_ITERATOR_H_
#define NEMO_INCLUDE_NEMO_VOLUME_ITERATOR_H_

#include "nemo_iterator.h"
#include "nemo.h"
#include <vector>

namespace nemo{

class VolumeIterator {
public:
    VolumeIterator( Nemo * nemo,const std::string  & start, const std::string & end, uint64_t limit = 1LL << 60, bool use_snapshot=true);
    virtual ~VolumeIterator();
    rocksdb::Slice key();
    int64_t value();
    char type();
//    virtual void Skip(int64_t offset);
    virtual void Next();
    virtual bool Valid();

    bool targetScan(int64_t target);
    int64_t totalVolume();
    std::string targetKey();

private:
    bool CheckLoad();
    void Init();    
    Nemo * n;
    HmetaIterator * Hit;
    LmetaIterator * Lit;
    SmetaIterator * Sit;
    ZmetaIterator * Zit;
    KIteratorRO   * Kit;
    bool    use_snapshot_;
    std::string end_;
    rocksdb::Slice ends_;
    uint64_t limit_;
    uint64_t count;
    bool valid_;
    std::vector<KVT> kvt;
    int64_t totalVol_;
    std::string targetKey_;
    
    //No Copying Allowed
    VolumeIterator(VolumeIterator&);
    void operator=(VolumeIterator&);
};

}
#endif