#include <stdio.h>
#include <string.h>  // memcpy
#include <sys/time.h>
#include <xmmintrin.h>

#include <algorithm>
#include <fstream>
#include <iostream>
#include <string>
#include <vector>
#include <time.h>

#include "include/common.hh"
#include "include/time_stamp.hh"
#include "include/transaction.hh"
#include "include/version.hh"

#include "../include/backoff.hh"
#include "../include/debug.hh"
#include "../include/masstree_wrapper.hh"
#include "../include/tsc.hh"

#define delta 0

void TxExecutor::begin() { 
  
  this->status_ = TransactionStatus::inflight; 
  
  __atomic_store_n(&(ThreadWtsArray[thid_].obj_), this->wts_.ts_, __ATOMIC_RELEASE);
  
}

void TxExecutor::read() {
#if ADD_ANALYSIS
  uint64_t start = rdtscp();
#endif  // if ADD_ANALYSIS

  Tuple *tuple;

  for (auto itr = read_operation_set_.begin(); itr != read_operation_set_.end(); ++itr) {
#if MASSTREE_USE
    tuple = MT.get_value(*itr);
    // read request to remote replica
#if ADD_ANALYSIS
  ++mres_->local_tree_traversal_;
#endif  // if ADD_ANALYSIS
#else
  tuple = get_tuple(Table, *itr);
#endif  // if MASSTREE_USE
    // avoid re-read and read-own write
    if (searchReadPairSet(*itr)){
      continue;
    }
    read_pair_set_.emplace_back(*itr, tuple);
  }


  for(auto itr = read_pair_set_.begin(); itr != read_pair_set_.end(); ++itr){

    // Search version 
    Version *ver, *later_ver;
    later_ver = nullptr;
    ver = (*itr).second->ldAcqLatest(); 


    //read operation read latest version less than its timestamp.
    while (ver->ldAcqWts() > this->wts_.ts_) { 
      later_ver = ver;               
      ver = ver->ldAcqNext();
      if (ver == nullptr) break; 
    }

    // critical section (avoiding interleaving write visible at cccheck)

    
    //validate version of tuple
    while(ver->status_.load(memory_order_acquire) == VersionStatus::aborted ||
          ver->status_.load(memory_order_acquire) == VersionStatus::invisible) {
      ver = ver->ldAcqNext();
    }
  
    if (this->wts_.ts_ > ver->ldAcqRts()){
          ver->rts_.store(this->wts_.ts_, memory_order_relaxed);
    }
    
    //read selected version
    memcpy(return_val_, ver->val_, VAL_SIZE);

    read_set_.emplace_back((*itr).first, (*itr).second, later_ver, ver);

    dependency_set_.emplace_back(ver->ldAcqWts(), ver);

  }

  read_operation_set_.clear();
  read_pair_set_.clear();

  #if ADD_ANALYSIS
    mres_->local_read_latency_ += rdtscp() - start;
  #endif

  return;
}

void TxExecutor::write(){
  
#if ADD_ANALYSIS
  uint64_t start = rdtscp();
#endif  // if ADD_ANALYSIS
  Tuple *tuple;

  for (auto itr = write_operation_set_.begin(); itr != write_operation_set_.end(); ++itr) { 
#if MASSTREE_USE
    tuple = MT.get_value(*itr);
    // read request to remote replica
#if ADD_ANALYSIS
  ++mres_->local_tree_traversal_;
#endif  // if ADD_ANALYSIS
#else
  tuple = get_tuple(Table, *itr);
#endif  // if MASSTREE_USE
    // avoid write own write
    if (searchWritePairSet(*itr)){
      continue;
    }
    write_pair_set_.emplace_back(*itr, tuple);
  }

  for(auto itr = write_pair_set_.begin(); itr != write_pair_set_.end(); ++itr){

    Version *expected(nullptr), *ver, *later_ver, *new_ver, *pre_ver;

    later_ver = nullptr;
    ver = (*itr).second->ldAcqLatest();

    // decide where new version are inserted in version list.
    while (ver->ldAcqWts() > this->wts_.ts_) {
      later_ver = ver;
      ver = ver->ldAcqNext();
      if (ver == nullptr) break;
    }

    new_ver = newVersionGeneration((*itr).second);

    for (;;) {
      if (later_ver) {
        pre_ver = later_ver;
        ver = pre_ver->ldAcqNext();
      } else {
        ver = expected = (*itr).second->ldAcqLatest();
      }
      //以下のwhile文が無いと、(ver == expected)の時、CASが失敗し続けてしまう可能性がある。
      while (ver->ldAcqWts() > this->wts_.ts_) {
        later_ver = ver;
        pre_ver = ver;
        ver = ver->ldAcqNext();
      }

      if (ver == expected) { //add latest version in version list 
        new_ver->strRelNext(expected);
        if ((*itr).second->latest_.compare_exchange_strong(expected, new_ver, memory_order_acq_rel, memory_order_acquire)) {
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
    write_set_.emplace_back((*itr).first, (*itr).second, later_ver, new_ver);
  }

  write_operation_set_.clear();
  write_pair_set_.clear();

#if ADD_ANALYSIS
  mres_->local_write_latency_ += rdtscp() - start;
#endif  // if ADD_ANALYSIS

  return;
  
}

void TxExecutor::CCcheck(){
#if ADD_ANALYSIS
  uint64_t start = rdtscp();
#endif  // if ADD_ANALYSIS

  for (auto itr = read_set_.begin(); itr != read_set_.end(); ++itr){
    if((*itr).rcdptr_->lock_.w_trylock()){
      locked_tuple_set_.emplace_back((*itr).rcdptr_);
    }
    else {
      this->status_ = TransactionStatus::abort;
      unlock();
      return;
    }
  }

  for (auto itr = write_set_.begin(); itr != write_set_.end(); ++itr){
    if (searchReadSet((*itr).rcdptr_)) continue;
    if((*itr).rcdptr_->lock_.w_trylock()){
      locked_tuple_set_.emplace_back((*itr).rcdptr_);
    }
    else {
      this->status_ = TransactionStatus::abort;
      unlock();
      return;
    }
  }
  
  Version *ver, *later_ver;

  // read check
  for (auto itr = read_set_.begin(); itr != read_set_.end(); ++itr) {

    ver = (*itr).rcdptr_->ldAcqLatest();
    later_ver = nullptr;

    std::vector<WriteElement<Tuple>> committedWrites;
    // version < ts(T')  
    while ((*itr).ver_->ldAcqWts() < ver->ldAcqWts()) { 
      if (ver->ldAcqStatus() == VersionStatus::committed || ver->ldAcqStatus() == VersionStatus::prepared){
        committedWrites.emplace_back((*itr).key_, (*itr).rcdptr_, later_ver, ver);
      } 
      later_ver = ver;               
      ver = ver->ldAcqNext(); 
      if (ver == nullptr) break;
    }
    // ts(T') < ts(T)
    for (auto committedWrite = committedWrites.begin(); committedWrite != committedWrites.end(); ++committedWrite){
      if ((*committedWrite).new_ver_->ldAcqWts() < this->wts_.ts_){
        committedWrites.clear();
        this->status_ = TransactionStatus::abort;
        unlock();
        return;
      }
    }

    committedWrites.clear();
  }
  
  //write check
  for (auto itr = write_set_.begin(); itr != write_set_.end(); ++itr) {

    ver = (*itr).rcdptr_->ldAcqLatest();
    later_ver = nullptr;  

    while (this->wts_.ts_ <= ver->ldAcqWts()){
      later_ver = ver;               
      ver = ver->ldAcqNext(); 
    }

    while(ver->ldAcqStatus() == VersionStatus::aborted || ver->ldAcqStatus() == VersionStatus::invisible){
      later_ver = ver;               
      ver = ver->ldAcqNext(); 
    }
    

    if (this->wts_.ts_ < ver->ldAcqRts()){
      this->status_ = TransactionStatus::abort;
      for (auto itr = write_set_.begin(); itr != write_set_.end(); ++itr){
        (*itr).new_ver_->status_.store( VersionStatus::aborted,
                                    std::memory_order_release);
      }
      unlock();
      return;
    }
  }

  // Prepared.add(T)
  for (auto itr = write_set_.begin(); itr != write_set_.end(); ++itr){
    (*itr).new_ver_->status_.store( VersionStatus::prepared,
                                    std::memory_order_release);
  }

  // Dependency check 
  for (auto itr = read_set_.begin(); itr != read_set_.end(); ++itr) {

    // wait for all pending dependencies
    while((*itr).ver_->ldAcqStatus() == VersionStatus::prepared){
    };

    // if dependent transaction abort
    if ((*itr).ver_->ldAcqStatus() == VersionStatus::aborted) {
      this->status_ = TransactionStatus::abort;
      for (auto itr = write_set_.begin(); itr != write_set_.end(); ++itr){
        (*itr).new_ver_->status_.store( VersionStatus::aborted,
                                    std::memory_order_release);
      }
      unlock();
      return ;
    }
  }

  this->status_ = TransactionStatus::commit;
  unlock(); 

#if ADD_ANALYSIS
  mres_->local_cccheck_latency_ += rdtscp() - start;
#endif  // if ADD_ANALYSIS
  
  return ;
}


void TxExecutor::abort(){

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

void TxExecutor::unlock(){

  for (auto itr = locked_tuple_set_.begin(); itr != locked_tuple_set_.end(); ++itr){
    (*itr)->lock_.w_unlock();
  }
  locked_tuple_set_.clear();

}
