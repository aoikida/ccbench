#pragma once

#include <stdio.h>
#include <atomic>
#include <iostream>
#include <map>
#include <queue>

#include "../../include/backoff.hh"
#include "../../include/config.hh"
#include "../../include/debug.hh"
#include "../../include/inline.hh"
#include "../../include/procedure.hh"
#include "../../include/result.hh"
#include "../../include/string.hh"
#include "../../include/util.hh"
#include "mvtso_op_element.hh"
#include "common.hh"
#include "time_stamp.hh"
#include "tuple.hh"
#include "version.hh"


enum class TransactionStatus : uint8_t {
  inflight,
  commit,
  abort,
};

class TxExecutor {
public:
  TransactionStatus status_ = TransactionStatus::inflight;
  TimeStamp wts_;
  std::vector<ReadElement<Tuple>> read_set_;
  std::vector<WriteElement<Tuple>> write_set_;
  std::vector<Procedure> pro_set_;
  std::vector<uint64_t> read_operation_set_;
  std::vector<uint64_t> write_operation_set_;
  std::vector<std::pair<uint64_t, Tuple*>> read_pair_set_;
  std::vector<std::pair<uint64_t, Version*>> dependency_set_;
  Result *mres_ = nullptr;
  std::vector<Tuple *> tuple_lock_list_;

  uint8_t thid_ = 0;
  uint64_t start_, stop_;

  char return_val_[VAL_SIZE] = {};
  char write_val_[VAL_SIZE] = {};

  TxExecutor(uint8_t thid, Result *mres) : mres_(mres), thid_(thid) {
    // wait to initialize MinWts
    wts_.generateTimeStampFirst(thid_);
    __atomic_store_n(&(ThreadWtsArray[thid_].obj_), wts_.ts_, __ATOMIC_RELEASE);

    unsigned int expected, desired;
    expected = FirstAllocateTimestamp.load(memory_order_acquire);
    for (;;) {
      desired = expected + 1;
      if (FirstAllocateTimestamp.compare_exchange_weak( expected, desired,
                                                        memory_order_acq_rel))
        break;
    }

    read_set_.reserve(FLAGS_max_ope);
    write_set_.reserve(FLAGS_max_ope);
    pro_set_.reserve(FLAGS_max_ope);

    genStringRepeatedNumber(write_val_, VAL_SIZE, thid_);

    start_ = rdtscp();

  }

  ~TxExecutor() {
    read_set_.clear();
    write_set_.clear();
    pro_set_.clear();
  }

  void begin();

  void read();

  void write(uint64_t key);

  void CCcheck();

  void abort();

  void commit();

  void unlock();

  Version *newVersionGeneration([[maybe_unused]] Tuple *tuple) {
    return new Version(this->wts_.ts_, this->wts_.ts_);
  }

  bool searchReadSet(const Tuple * tuple) {
    for (auto itr = read_set_.begin(); itr != read_set_.end(); ++itr) {
      if ((*itr).rcdptr_ == tuple) return true;
    } 
    return false;
  }

  WriteElement<Tuple> *searchWriteSet(const uint64_t key) {
    for (auto itr = write_set_.begin(); itr != write_set_.end(); ++itr) {
      if ((*itr).key_ == key) return &(*itr);
    }

    return nullptr;
  }

  std::pair<uint64_t, Tuple*> *searchReadPairSet(const uint64_t key) {
    for (auto itr = read_pair_set_.begin(); itr != read_pair_set_.end(); ++itr){
      if ((*itr).first == key) return &(*itr);
    }
    return nullptr;
  }

  static INLINE Tuple *get_tuple(Tuple *table, uint64_t key) {
    return &table[key];
  }
};


