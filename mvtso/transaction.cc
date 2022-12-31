#include <stdio.h>
#include <string.h>  // memcpy
#include <sys/time.h>
#include <xmmintrin.h>

#include <algorithm>
#include <fstream>
#include <iostream>
#include <string>
#include <vector>

#include "include/common.hh"
#include "include/time_stamp.hh"
#include "include/transaction.hh"
#include "include/version.hh"

#include "../include/backoff.hh"
#include "../include/debug.hh"
#include "../include/masstree_wrapper.hh"
#include "../include/tsc.hh"

#define delta  0

void TxExecutor::begin() { 
  
  this->status_ = TransactionStatus::inflight; 
  
  this->wts_.generateTimeStamp(thid_);
  __atomic_store_n(&(ThreadWtsArray[thid_].obj_), this->wts_.ts_, __ATOMIC_RELEASE);
  
}

void TxExecutor::read(const uint64_t key) {

  #if ADD_ANALYSIS
  uint64_t start = rdtscp();
#endif  // if ADD_ANALYSIS

  Tuple *tuple;
  /**
   * read-own-writes or re-read from local read set.
   */
  if (searchWriteSet(key) || searchReadSet(key)) goto FINISH_READ;

  /**
   * Search versions from data structure.
   */
#if MASSTREE_USE
  tuple = MT.get_value(key);
  // read request to remote replica
  usleep(1);

#if ADD_ANALYSIS
  ++mres_->local_tree_traversal_;
#endif  // if ADD_ANALYSIS

#else
  tuple = get_tuple(Table, key);
  //read request to remote replica
  usleep(1);
#endif  // if MASSTREE_USE

 
  // Search version 
  Version *ver, *later_ver;
  later_ver = nullptr;
  ver = tuple->ldAcqLatest(); 
  
  //read operation read latest version less than its timestamp.
  while (ver->ldAcqWts() > this->wts_.ts_) { 
    later_ver = ver;               
    ver = ver->ldAcqNext();
    if (ver == nullptr) break;
  }

  //validate version of tuple
  while(ver->status_.load(memory_order_acquire) == VersionStatus::aborted ||
        ver->status_.load(memory_order_acquire) == VersionStatus::invisible) {
    ver = ver->ldAcqNext();
  }
  

  //read selected version
  memcpy(return_val_, ver->val_, VAL_SIZE);

  //update RTS of the version
  ver->rts_.store(this->wts_.ts_, memory_order_relaxed);

  read_set_.emplace_back(key, tuple, later_ver, ver);

  dependency_set_.emplace_back(ver->ldAcqWts(), ver);

#if ADD_ANALYSIS
  mres_->local_read_latency_ += rdtscp() - start;
#endif  // if ADD_ANALYSIS

FINISH_READ:

#if ADD_ANALYSIS
  mres_->local_read_latency_ += rdtscp() - start;
#endif

  return;
}

void TxExecutor::write(const uint64_t key){
  
#if ADD_ANALYSIS
  uint64_t start = rdtscp();
#endif  // if ADD_ANALYSIS
  Tuple *tuple;

  Version *expected(nullptr), *ver, *later_ver, *new_ver, *pre_ver;
  
  if (searchWriteSet(key)) goto FINISH_WRITE;

#if MASSTREE_USE
  tuple = MT.get_value(key);

#if ADD_ANALYSIS
  ++mres_->local_tree_traversal_;
#endif  // if ADD_ANALYSIS

#else
  tuple = get_tuple(Table, key);
#endif  // if MASSTREE_USE

  later_ver = nullptr;
  ver = tuple->ldAcqLatest();

  // decide where new version are inserted in version list.
  while (ver->ldAcqWts() > this->wts_.ts_) {
    later_ver = ver;
    ver = ver->ldAcqNext();
    if (ver == nullptr) break;
  }

  new_ver = newVersionGeneration(tuple);

  
  //add new version in version list
  for (;;) {
    if (later_ver) {
      pre_ver = later_ver;
      ver = pre_ver->ldAcqNext();
    } else {
      ver = expected = tuple->ldAcqLatest();
    }
    //以下のwhile文が無いと、(ver == expected)の時、CASが失敗し続けてしまう可能性がある。
    while (ver->ldAcqWts() > this->wts_.ts_) {
      later_ver = ver;
      pre_ver = ver;
      ver = ver->ldAcqNext();
    }

    if (ver == expected) { //add latest version in version list
      new_ver->strRelNext(expected);
      if (tuple->latest_.compare_exchange_strong(expected, new_ver, memory_order_acq_rel, memory_order_acquire)) {
        break;
      }
    } 
    else { //add version in version list (except latest version)
      new_ver->strRelNext(ver);
      if (pre_ver->next_.compare_exchange_strong(ver, new_ver, memory_order_acq_rel, memory_order_acquire)) {
        break;
      }
    }
  }
  

  write_set_.emplace_back(key, tuple, later_ver, new_ver);

FINISH_WRITE:

#if ADD_ANALYSIS
  mres_->local_write_latency_ += rdtscp() - start;
#endif  // if ADD_ANALYSIS
  return;
  
}

void TxExecutor::CCcheck(){
#if ADD_ANALYSIS
  uint64_t start = rdtscp();
#endif  // if ADD_ANALYSIS

  Version *ver, *later_ver;
  uint64_t txts;

  // byzantine timestamp check
  // ts(T) > localclock + δ
  if (this->wts_.ts_ > ((rdtscp() << (sizeof(thid_) * 8)) | thid_) + delta){
    this->status_ = TransactionStatus::abort;
    return;
  }
  
  // byzantine dependency check
  for (auto itr = dependency_set_.begin(); itr != dependency_set_.end(); ++itr){
    txts = (*itr).first;
    ver = (*itr).second;
    
    if (txts != ver->ldAcqWts()){
      this->status_ = TransactionStatus::abort;
      return;
    }
    
    if (ver->ldAcqStatus() == VersionStatus::invisible || ver->ldAcqStatus() == VersionStatus::aborted){
      this->status_ = TransactionStatus::abort;
      return;
    }
    
  }

  // read check
  for (auto itr = read_set_.begin(); itr != read_set_.end(); ++itr) {

    ver = (*itr).rcdptr_->ldAcqLatest();
    later_ver = nullptr;
    
    std::vector<WriteElement<Tuple>> committedWrites;
    // version < ts(T')  
    while ((*itr).ver_->wts_ < ver->ldAcqWts()) { 
      if (ver->ldAcqStatus() == VersionStatus::committed || ver->ldAcqStatus() == VersionStatus::prepared){
        committedWrites.emplace_back((*itr).key_, (*itr).rcdptr_, later_ver, ver);
      } 
      later_ver = ver;               
      ver = ver->ldAcqNext(); 
      if (ver == nullptr) break;
    }
    // ts(T') < ts(T)
    for (auto committedWrite = committedWrites.begin(); committedWrite != committedWrites.end(); ++committedWrite){
      if ((*committedWrite).new_ver_->wts_ < this->wts_.ts_){
        committedWrites.clear();
        this->status_ = TransactionStatus::abort;
        return;
      }
    }

    committedWrites.clear();
  }
  
  //write check
  for (auto itr = write_set_.begin(); itr != write_set_.end(); ++itr) {

    ver = (*itr).rcdptr_->ldAcqLatest();
    later_ver = nullptr;  

    std::vector<ReadElement<Tuple>> committedReads;
  
    // TS(T) < TS(T') 
    while (this->wts_.ts_ < ver->ldAcqRts()) { 
      if (ver->ldAcqStatus() == VersionStatus::committed || ver->ldAcqStatus() == VersionStatus::prepared){
        committedReads.emplace_back((*itr).key_, (*itr).rcdptr_, later_ver, ver);
      }
      later_ver = ver;               
      ver = ver->ldAcqNext(); 
      if (ver == nullptr) break;
    }

    //ReadSet(t')[key].version < TS(T)
    for (auto committedRead = committedReads.begin(); committedRead != committedReads.end(); ++committedRead){
      if ((*committedRead).ver_->wts_ < this->wts_.ts_){
        committedReads.clear();
        this->status_ = TransactionStatus::abort;
        return;
      }
    }

    committedReads.clear();

    ver = (*itr).rcdptr_->ldAcqLatest();
    later_ver = nullptr;
    
    // key.RTS > ts(T)
    while (ver->ldAcqStatus() == VersionStatus::invisible){
      if (ver->ldAcqRts() > this->wts_.ts_){
        this->status_ = TransactionStatus::abort;
        return;
      }
      later_ver = ver;
      ver = ver->ldAcqNext();
      if (ver == nullptr) break;
    }
#if ADD_ANALYSIS
  mres_->local_cccheck_latency_ += rdtscp() - start;
#endif  // if ADD_ANALYSIS
  }

  // Prepared.add(T)
  for (auto itr = write_set_.begin(); itr != write_set_.end(); ++itr){
    (*itr).new_ver_->status_.store( VersionStatus::prepared,
                                    std::memory_order_release);
  }

  // Dependency check 
  for (auto itr = read_set_.begin(); itr != read_set_.end(); ++itr) {

    // wait for all pending dependencies
    while((*itr).ver_->ldAcqStatus() == VersionStatus::prepared){};

    // if dependent transaction abort
    if ((*itr).ver_->ldAcqStatus() == VersionStatus::aborted) {
      this->status_ = TransactionStatus::abort;
      return ;
    }
  }
  
  this->status_ = TransactionStatus::commit;
  //std::cout << "4" << std::endl;
  return ;
}

void TxExecutor::abort(){
  
  for (auto itr = write_set_.begin(); itr != write_set_.end(); ++itr){
    (*itr).new_ver_->status_.store( VersionStatus::aborted,
                                    std::memory_order_release);
  }

  read_set_.clear();
  write_set_.clear();
  dependency_set_.clear();

  this->wts_.set_clockBoost(FLAGS_clocks_per_us); 
  
}

void TxExecutor::commit(){
  
  for (auto itr = write_set_.begin(); itr != write_set_.end(); ++itr){
    (*itr).new_ver_->status_.store( VersionStatus::committed,
                                    std::memory_order_release);
  }

  read_set_.clear();
  write_set_.clear();
  dependency_set_.clear();

  this->wts_.set_clockBoost(0);
  
}
