#pragma once

#include <iostream>
#include <set>
#include <vector>

#include "common.hpp"
#include "log.hpp"
#include "silo_op_element.hpp"
#include "result.hpp"
#include "tuple.hpp"

#include "../../include/fileio.hpp"
#include "../../include/procedure.hpp"
#include "../../include/string.hpp"

#define LOGSET_SIZE 1000

using namespace std;

class TxnExecutor {
public:
  vector<ReadElement<Tuple>> readSet;
  vector<WriteElement<Tuple>> writeSet;
  vector<Procedure> proSet;

  vector<LogRecord> logSet;
  LogHeader latestLogHeader;

  unsigned int thid;
  SiloResult* rsob;

  File logfile;

  Tidword mrctid;
  Tidword max_rset, max_wset;

  char writeVal[VAL_SIZE];
  char returnVal[VAL_SIZE];

  TxnExecutor(int thid_, SiloResult* rsob_) : thid(thid_), rsob(rsob_) {
    readSet.reserve(MAX_OPE);
    writeSet.reserve(MAX_OPE);
    proSet.reserve(MAX_OPE);
    //logSet.reserve(LOGSET_SIZE);

    //latestLogHeader.init();

    max_rset.obj = 0;
    max_wset.obj = 0;

    genStringRepeatedNumber(writeVal, VAL_SIZE, thid);
  }

  void display_write_set();
  void tbegin();
  char* tread(uint64_t key);
  void twrite(uint64_t key);
  bool validationPhase();
  void abort();
  void writePhase();
  void wal(uint64_t ctid);
  void lockWriteSet();
  void unlockWriteSet();
  ReadElement<Tuple> *searchReadSet(uint64_t key);
  WriteElement<Tuple> *searchWriteSet(uint64_t key);

  Tuple* get_tuple(Tuple *table, uint64_t key) {
    return &table[key];
  }
};
