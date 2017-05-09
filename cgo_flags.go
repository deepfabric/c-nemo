package cnemo
// #cgo CPPFLAGS: -Iinternal/include
// #cgo CPPFLAGS: -Iinternal/src
// #cgo CPPFLAGS: -Iinternal/3rdparty/rocksdb/include -Iinternal/3rdparty/rocksdb -Iinternal/3rdparty/rocksdb/db
// #cgo CPPFLAGS: -Iinternal/3rdparty/rocksdb/util
// #cgo CPPFLAGS: -Iinternal/3rdparty/rocksdb/utilities/merge_operators/string_append
// #cgo CXXFLAGS: -std=c++11
// #cgo !darwin LDFLAGS: -Wl,-unresolved-symbols=ignore-all
import "C"
