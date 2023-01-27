#include <atomic>
#include <bitset>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <iostream>

// ccbench/cicada/include
#include "include/common.hh"
#include "include/time_stamp.hh"
#include "include/transaction.hh"
#include "include/util.hh"

// ccbench/include/
#include "backoff.hh"
#include "config.hh"
#include "logger.h"
#include "masstree_wrapper.hh"

using std::cout, std::endl;

void chkArg() {
    displayParameter();

    if (FLAGS_rratio > 100) {
        SPDLOG_INFO("rratio [%%] must be 0 ~ 100)");
        ERR;
    }

    if (FLAGS_zipf_skew >= 1) {
        SPDLOG_INFO("FLAGS_zipf_skew must be 0 ~ 0.999...");
        ERR;
    }

    if (FLAGS_clocks_per_us < 100) {
        SPDLOG_INFO("CPU_MHZ is less than 100. are you really?\n");
        exit(0);
    }

    if (posix_memalign((void**) &ThreadWtsArray, CACHE_LINE_SIZE,
                       FLAGS_thread_num * sizeof(uint64_t_64byte)) != 0)
        ERR;
    if (posix_memalign((void**) &ThreadRtsArray, CACHE_LINE_SIZE,
                       FLAGS_thread_num * sizeof(uint64_t_64byte)) != 0)
        ERR;

    // init
    for (unsigned int i = 0; i < FLAGS_thread_num; ++i) {
        ThreadRtsArray[i].obj_ = 0;
        ThreadWtsArray[i].obj_ = 0;
    }
}

[[maybe_unused]] void displayDB() {
    Tuple* tuple;
    Version* version;

    for (unsigned int i = 0; i < FLAGS_tuple_num; ++i) {
        tuple = &Table[i % FLAGS_tuple_num];
        cout << "------------------------------" << endl;  //-は30個
        cout << "key: " << i << endl;

        version = tuple->latest_;
        while (version != nullptr) {
            cout << "val: " << version->val_ << endl;

            switch (version->status_) {
                case VersionStatus::invisible:
                    cout << "status:  invisible";
                    break;
                case VersionStatus::prepared:
                    cout << "status:  prepared";
                    break;
                case VersionStatus::aborted:
                    cout << "status:  aborted";
                    break;
                case VersionStatus::committed:
                    cout << "status:  committed";
                    break;
                default:
                    cout << "status error";
                    break;
            }
            cout << endl;

            cout << "wts: " << version->wts_ << endl;
            cout << "bit: " << static_cast<bitset<64> >(version->wts_) << endl;
            cout << endl;

            version = version->next_;
        }
        cout << endl;
    }
}

void displayParameter() {
    cout << "#FLAGS_clocks_per_us:\t\t\t" << FLAGS_clocks_per_us << endl;
    cout << "#FLAGS_extime:\t\t\t\t" << FLAGS_extime << endl;
    cout << "#FLAGS_comm_time_ms:\t\t\t"<< FLAGS_comm_time_ms << endl;
    cout << "#FLAGS_io_time_ns:\t\t\t" << FLAGS_io_time_ns << endl;
    cout << "#FLAGS_max_ope:\t\t\t\t" << FLAGS_max_ope << endl;
    cout << "#FLAGS_rmw:\t\t\t\t" << FLAGS_rmw << endl;
    cout << "#FLAGS_rratio:\t\t\t\t" << FLAGS_rratio << endl;
    cout << "#FLAGS_thread_num:\t\t\t" << FLAGS_thread_num << endl;
    cout << "#FLAGS_tuple_num:\t\t\t" << FLAGS_tuple_num << endl;
    cout << "#FLAGS_ycsb:\t\t\t\t" << FLAGS_ycsb << endl;
    cout << "#FLAGS_zipf_skew:\t\t\t" << FLAGS_zipf_skew << endl;
    cout << "#FLAGS_batch_size:\t\t\t" << FLAGS_batch_size << endl;
}

[[maybe_unused]] void displayThreadWtsArray() {
    cout << "ThreadWtsArray:" << endl;
    for (unsigned int i = 0; i < FLAGS_thread_num; i++) {
        cout << "thid " << i << ": " << ThreadWtsArray[i].obj_ << endl;
    }
    cout << endl << endl;
}

[[maybe_unused]] void displayThreadRtsArray() {
    cout << "ThreadRtsArray:" << endl;
    for (unsigned int i = 0; i < FLAGS_thread_num; i++) {
        cout << "thid " << i << ": " << ThreadRtsArray[i].obj_ << endl;
    }
    cout << endl << endl;
}

void partTableInit([[maybe_unused]] size_t thid, uint64_t initts,
                   uint64_t start, uint64_t end) {
#if MASSTREE_USE
    MasstreeWrapper<Tuple>::thread_init(thid);
#endif

    for (uint64_t i = start; i <= end; ++i) {
        Tuple* tuple;
        tuple = TxExecutor::get_tuple(Table, i);
        tuple->latest_.store(new Version(), std::memory_order_release);
        (tuple->latest_.load(std::memory_order_acquire))
            ->set(0, initts, nullptr, VersionStatus::committed);
        (tuple->latest_.load(std::memory_order_acquire))->val_[0] = '\0';


#if MASSTREE_USE
        MT.insert_value(i, tuple);
#endif
    }
}

void partTableDelete([[maybe_unused]] size_t thid, uint64_t start,
                     uint64_t end) {
    for (uint64_t i = start; i <= end; ++i) {
        Tuple* tuple;
        tuple = TxExecutor::get_tuple(Table, i);
        Version* ver = tuple->latest_;
        while (ver != nullptr) {
            Version* del = ver;
            ver = ver->next_.load(memory_order_acquire);
            delete del;
        }
    }
}

void deleteDB() {
    size_t maxthread = decideParallelBuildNumber(FLAGS_tuple_num);
    std::vector<std::thread> thv;
    for (size_t i = 0; i < maxthread; ++i)
        thv.emplace_back(partTableDelete, i, i * (FLAGS_tuple_num / maxthread),
                         (i + 1) * (FLAGS_tuple_num / maxthread) - 1);
    for (auto &th : thv) th.join();

    delete Table;
    delete ThreadWtsArray;
    delete ThreadRtsArray;
}

void makeDB(uint64_t* initial_wts) {
    if (posix_memalign((void**) &Table, PAGE_SIZE, FLAGS_tuple_num * sizeof(Tuple)) !=
        0)
        ERR;
#if dbs11
    if (madvise((void *)Table, (FLAGS_tuple_num) * sizeof(Tuple), MADV_HUGEPAGE) != 0)
      ERR;
#endif

    TimeStamp tstmp;
    tstmp.generateTimeStampFirst(0);
    *initial_wts = tstmp.ts_;

    size_t maxthread = decideParallelBuildNumber(FLAGS_tuple_num);
    std::vector<std::thread> thv;
    for (size_t i = 0; i < maxthread; ++i)
        thv.emplace_back(partTableInit, i, tstmp.ts_, i * (FLAGS_tuple_num / maxthread),
                         (i + 1) * (FLAGS_tuple_num / maxthread) - 1);
    for (auto &th : thv) th.join();
}