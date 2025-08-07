#pragma once

#include <emmintrin.h>
#include <pthread.h>
#include <cmath>
#include <cstdint>
#include <iostream>
#include <thread>
#include "benchmark_object_64.h"
#include "benchmark_object_type.h"
#include "iceberg_table.h"

namespace tinyptr {

class BenchmarkIceberg : public BenchmarkObject64 {
   public:
    static const BenchmarkObjectType TYPE;

   private:
    uint64_t auto_iceberg_slot_num(uint64_t size);

   public:
    BenchmarkIceberg(int n);

    ~BenchmarkIceberg() = default;

    uint8_t Insert(uint64_t key, uint64_t value);
    uint64_t Query(uint64_t key, uint8_t ptr);
    void Update(uint64_t key, uint8_t ptr, uint64_t value);
    void Erase(uint64_t key, uint8_t ptr);

    void YCSBFill(std::vector<uint64_t>& keys, int num_threads);
    void YCSBRun(std::vector<std::pair<uint64_t, uint64_t>>& ops,
                 int num_threads);
    std::vector<std::tuple<uint64_t, double, uint64_t>> YCSBRunWithLatencyRecording(
        std::vector<std::pair<uint64_t, uint64_t>>& ops, int num_threads, uint64_t record_num,
        const std::vector<double>& percentiles);

    void ConcurrentRun(
        std::vector<std::tuple<uint64_t, uint64_t, uint64_t>>& ops,
        int num_threads);

   private:
    pid_t tid;
    iceberg_table tab;
};

}  // namespace tinyptr
