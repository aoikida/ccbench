#pragma once

#include <pthread.h>
#include <atomic>
#include <iostream>
#include <queue>

#include "../../include/cache_line_size.hh"
#include "../../include/int64byte.hh"
#include "../../include/masstree_wrapper.hh"
#include "tuple.hh"
#include "version.hh"

#include "gflags/gflags.h"
#include "glog/logging.h"

#ifdef GLOBAL_VALUE_DEFINE
#define GLOBAL
alignas(CACHE_LINE_SIZE) GLOBAL std::atomic<unsigned int> FirstAllocateTimestamp(0);
#if MASSTREE_USE
alignas(CACHE_LINE_SIZE) GLOBAL MasstreeWrapper<Tuple> MT; // NOLINT
#endif
#else
#define GLOBAL extern
alignas(CACHE_LINE_SIZE) GLOBAL std::atomic<unsigned int> FirstAllocateTimestamp;
#if MASSTREE_USE
alignas(CACHE_LINE_SIZE) GLOBAL MasstreeWrapper<Tuple> MT;
#endif
#endif

#ifdef GLOBAL_VALUE_DEFINE
DEFINE_uint64(clocks_per_us, 2133, "CPU_MHz. Use this info for measuring time."); // NOLINT
DEFINE_uint64(extime, 10, "Execution time[sec]."); // NOLINT
DEFINE_uint64(comm_time_ms, 1, "communication time[milisec]."); // NOLINT
DEFINE_uint64(io_time_ns, 5, "Delay inserted instead of IO."); // NOLINT
DEFINE_uint64(max_ope, 10, // NOLINT
              "Total number of operations per single transaction.");
DEFINE_bool(rmw, false, // NOLINT
            "True means read modify write, false means blind write.");
DEFINE_uint64(rratio, 50, "read ratio of single transaction."); // NOLINT
DEFINE_uint64(thread_num, 144, "Total number of worker threads."); // NOLINT
DEFINE_uint64(tuple_num, 1000000, "Total number of records."); // NOLINT
DEFINE_bool(ycsb, true, // NOLINT
            "True uses zipf_skew, false uses faster random generator.");
DEFINE_double(zipf_skew, 0, "zipf skew. 0 ~ 0.999..."); // NOLINT
DEFINE_uint64(batch_size, 1, "Size of batch"); // NOLINT
#else
DECLARE_uint64(clocks_per_us);
DECLARE_uint64(extime);
DECLARE_uint64(comm_time_ms);
DECLARE_uint64(io_time_ns);
DECLARE_uint64(max_ope);
DECLARE_bool(rmw);
DECLARE_uint64(rratio);
DECLARE_uint64(thread_num);
DECLARE_uint64(tuple_num);
DECLARE_bool(ycsb);
DECLARE_double(zipf_skew);
DECLARE_uint64(batch_size);
#endif

alignas(CACHE_LINE_SIZE) GLOBAL uint64_t_64byte* ThreadWtsArray;
alignas(CACHE_LINE_SIZE) GLOBAL uint64_t_64byte* ThreadRtsArray;

alignas(CACHE_LINE_SIZE) GLOBAL Tuple* Table;
[[maybe_unused]] alignas(CACHE_LINE_SIZE) GLOBAL uint64_t InitialWts;