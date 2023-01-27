#pragma once

#include <vector>

#include "../../include/backoff.hh"

extern void chkArg();

extern void deleteDB();

[[maybe_unused]] extern void displayDB();

extern void displayParameter();

[[maybe_unused]] extern void displayThreadWtsArray();

[[maybe_unused]] extern void displayThreadRtsArray();

extern void makeDB(uint64_t *initial_wts);

extern void partTableDelete([[maybe_unused]] size_t thid, uint64_t start,
                            uint64_t end);

extern void partTableInit([[maybe_unused]] size_t thid, uint64_t initts,
                          uint64_t start, uint64_t end);