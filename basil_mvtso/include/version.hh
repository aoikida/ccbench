#pragma once

#include <stdio.h>
#include <string.h>  // memcpy
#include <sys/time.h>

#include <atomic>
#include <cstdint>

#include "../../include/cache_line_size.hh"
#include "../../include/op_element.hh"
#include "rwlock.hh"

using namespace std;

enum class VersionStatus : uint8_t {
  invisible,
  prepared,
  committed,
  aborted
};

class Version {
public:
  alignas(CACHE_LINE_SIZE) atomic <uint64_t> rts_;
  atomic <uint64_t> wts_;
  atomic<Version *> next_;
  atomic <VersionStatus> status_;  // commit record
  RWLock lock_;

  char val_[10000]; //VAL_SIZE = 100

  Version() {
    status_.store(VersionStatus::invisible, memory_order_release);
    next_.store(nullptr, memory_order_release);
  }

  Version(const uint64_t rts, const uint64_t wts) {
    rts_.store(rts, memory_order_relaxed);
    wts_.store(wts, memory_order_relaxed);
    status_.store(VersionStatus::invisible, memory_order_release);
    next_.store(nullptr, memory_order_release);
  }
  /*
  void displayInfo() {
    printf(
            "Version::displayInfo(): this: %p rts_: %lu: wts_: %lu: next_: %p: "
            "status_: "
            "%u\n",
            this, ldAcqRts(), ldAcqWts(), ldAcqNext(), (uint8_t) ldAcqStatus());
  }
  */

  Version *ldAcqNext() { return next_.load(std::memory_order_acquire); }

  uint64_t ldAcqRts() { return rts_.load(std::memory_order_acquire); }

  VersionStatus ldAcqStatus() {
    return status_.load(std::memory_order_acquire);
  }

  uint64_t ldAcqWts() { 
    return wts_.load(std::memory_order_acquire); 
  }

  void set(const uint64_t rts, const uint64_t wts) {
    rts_.store(rts, memory_order_relaxed);
    wts_.store(wts, memory_order_relaxed);
    status_.store(VersionStatus::invisible, memory_order_release);
    next_.store(nullptr, memory_order_release);
  }

  void set(const uint64_t rts, const uint64_t wts, Version *next,
           const VersionStatus status) {
    rts_.store(rts, memory_order_relaxed);
    wts_.store(wts, memory_order_relaxed);
    status_.store(status, memory_order_release);
    next_.store(next, memory_order_release);
  }

  void strRelNext(Version *next) {  // store release next = strRelNext
    next_.store(next, std::memory_order_release);
  }

};