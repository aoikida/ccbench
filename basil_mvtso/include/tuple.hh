#pragma once

#include <atomic>
#include <cstdint>

#include "../../include/cache_line_size.hh"

#include "version.hh"
#include "../../include/rwlock.hh"

using namespace std;

class Tuple {
public:
  alignas(CACHE_LINE_SIZE)
  atomic<Version *> latest_; //このタプルの最新バージョン
  RWLock lock_;

  Tuple() : latest_(nullptr){}

  Version *ldAcqLatest() {  //このタプルの最新バージョンを取得する。
    return latest_.load(std::memory_order_acquire); 
  }

};