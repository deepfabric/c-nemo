GCC = g++
CPPFLAGS = -DROCKSDB_PLATFORM_POSIX -DROCKSDB_LIB_IO_POSIX  -DOS_LINUX -Wall -W -Wno-unused-parameter -DDEBUG -D__XDEBUG__ -g -O2 -std=c++11
OBJECT = meta_scan hash_meta_check

LIB_PATH = -L ../../output/lib
			
LIBS = -Wl,-Bstatic -lnemo -lnemodb -lrocksdb \
	   -Wl,-Bdynamic -lpthread\
	   -lsnappy \
	   -lrt \
	   -lz \
	   -lbz2 \
	   -ljemalloc

INCLUDE_PATH = -I../../output/include/ \
							 -I../../3rdparty/nemo-rocksdb/rocksdb/ \
							 -I../../3rdparty/nemo-rocksdb/rocksdb/include

OBJS = meta_scan.o hash_meta_check.o

.PHONY: all clean


# BASE_BOJS := $(wildcard *.cpp)
# BASE_BOJS += $(wildcard *.c)
# OBJS := $(patsubst %.cpp,%.o,$(BASE_BOJS)) 


all: $(OBJECT)
	rm *.o

meta_scan: meta_scan.o
	$(CXX) $(CXXFLAGS) -o $@ $^ $(INCLUDE_PATH) $(LIB_PATH) $(LIBS)

hash_meta_check: hash_meta_check.o
	$(CXX) $(CXXFLAGS) -o $@ $^ $(INCLUDE_PATH) $(LIB_PATH) $(LIBS)

$(OBJS): %.o : %.cc
	$(GCC) $(CPPFLAGS) -c $< -o $@ $(INCLUDE_PATH)

clean:
	rm -rf $(OBJECT) $(OBJECT).o
