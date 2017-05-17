#ifndef NEMO_INCLUDE_NEMO_VOLUME_ITERATOR_H_
#define NEMO_INCLUDE_NEMO_VOLUME_ITERATOR_H_

#include "nemo_iterator.h"
#include "nemo.h"
#include <vector>

namespace nemo{

class VolumeIterator {
public:
    VolumeIterator( Nemo * nemo,const std::string  & start, const std::string & end, uint64_t limit, bool use_snapshot=true);
    virtual ~VolumeIterator();
    std::string key();
    int64_t value();
    char type();
//    virtual void Skip(int64_t offset);
    virtual void Next();
    virtual bool Valid();

private:
    bool CheckLoad();
    void Init();    
    Nemo * n;
    HmetaIterator * Hit;
    LmetaIterator * Lit;
    SmetaIterator * Sit;
    ZmetaIterator * Zit;
    KIterator     * Kit;
    bool    use_snapshot_;
    std::string end_;
    uint64_t limit_;
    uint64_t count;
    bool valid_;
    std::vector<KVT> kvt;

    //No Copying Allowed
    VolumeIterator(VolumeIterator&);
    void operator=(VolumeIterator&);
};

}
#endif