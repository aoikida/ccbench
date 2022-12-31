#include <pthread.h>
#include <cstdlib>
#include <cstring>
#include <algorithm>
#include <cstdint>
#include <functional>
#include <thread>

#define GLOBAL_VALUE_DEFINE

#include "../include/atomic_wrapper.hh"
#include "../include/backoff.hh"
#include "include/common.hh"
#include "include/result.hh"
#include "include/transaction.hh"
#include "include/util.hh"


void
worker(size_t thid, char &ready, const bool &start, const bool &quit) { 

	Xoroshiro128Plus rnd;
  rnd.init();
  TxExecutor trans(thid, (Result*) &MVTSOResult[thid]);
  Result &myres = std::ref(MVTSOResult[thid]);
  FastZipf zipf(&rnd, FLAGS_zipf_skew, FLAGS_tuple_num);

	#ifdef Linux
    setThreadAffinity(thid);
    // printf("Thread #%d: on CPU %d\n", *myid, sched_getcpu());
    // printf("sysconf(_SC_NPROCESSORS_CONF) %d\n",
    // sysconf(_SC_NPROCESSORS_CONF));
	#endif  // Linux

	#ifdef Darwin
    int nowcpu;
    GETCPU(nowcpu);
    // printf("Thread %d on CPU %d\n", *myid, nowcpu);
	#endif  // Darwin
	storeRelease(ready, 1);
  while (!loadAcquire(start)) _mm_pause();
  while (!loadAcquire(quit)){
#if PARTITION_TABLE
        makeProcedure(trans.pro_set_, rnd, zipf, FLAGS_tuple_num, FLAGS_max_ope, FLAGS_thread_num,
                      FLAGS_rratio, FLAGS_rmw, FLAGS_ycsb, true, thid, myres);
#else
        makeProcedure(trans.pro_set_, rnd, zipf, FLAGS_tuple_num, FLAGS_max_ope, FLAGS_thread_num,
                      FLAGS_rratio, FLAGS_rmw, FLAGS_ycsb, false, thid, myres);
#endif
		RETRY:
        if (loadAcquire(quit)) break;

        trans.begin();
        for (auto &&itr : trans.pro_set_) {
            if ((itr).ope_ == Ope::READ) {
                trans.read((itr).key_);
            } else if ((itr).ope_ == Ope::WRITE) {
                trans.write((itr).key_);
            } else if ((itr).ope_ == Ope::READ_MODIFY_WRITE) {
                trans.read((itr).key_);
                trans.write((itr).key_);
            } else {
                ERR;
            }
        }
        trans.CCcheck();

        if (trans.status_ == TransactionStatus::abort) {
              trans.abort();
              storeRelease(myres.local_abort_counts_,
                         loadAcquire(myres.local_abort_counts_) + 1);
              goto RETRY;
        }
        else if (trans.status_ == TransactionStatus::commit){
          trans.commit();
          storeRelease(myres.local_commit_counts_,
                         loadAcquire(myres.local_commit_counts_) + 1);
        }
        else{
          std::cout << "error stauts" << std::endl;
        }
	}
}

int
main(int argc, char *argv[])
{	
	gflags::SetUsageMessage("MVTSO benchmark.");
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  chkArg();
	uint64_t initial_wts;
	makeDB(&initial_wts);
	
	alignas(CACHE_LINE_SIZE) bool start = false;
  alignas(CACHE_LINE_SIZE) bool quit = false;
  initResult();
  std::vector<char> readys(FLAGS_thread_num);
  std::vector<std::thread> thv;
  for (size_t i = 0; i < FLAGS_thread_num; ++i)
      thv.emplace_back(worker, i, std::ref(readys[i]), std::ref(start),
                       std::ref(quit));
  waitForReady(readys);
  storeRelease(start, true);
  for (size_t i = 0; i < FLAGS_extime; ++i) {
      sleepMs(1000);
  }
  storeRelease(quit, true);
  for (auto &th : thv) th.join();
  for (unsigned int i = 0; i < FLAGS_thread_num; ++i) {
      MVTSOResult[0].addLocalAllResult(MVTSOResult[i]);
  }
	MVTSOResult[0].displayAllResult(FLAGS_clocks_per_us, FLAGS_extime, FLAGS_thread_num);
  deleteDB();

  return 0;
}