#pragma once

#include <emmintrin.h>
#include <pthread.h>
#include <cmath>
#include <cstdint>
#include <iostream>
#include <thread>
#include "benchmark_object_64.h"
#include "benchmark_object_type.h"
#include "resizable_ht.h"

namespace tinyptr {

class BenchmarkResizableSkulkerHT : public BenchmarkObject64 {
   public:
    static const BenchmarkObjectType TYPE;

   public:
    BenchmarkResizableSkulkerHT(uint64_t initial_size_per_part_,
                                uint64_t part_num_, uint32_t thread_num_ = 0,
                                double resize_threshold_ = 0.75,
                                double resize_factor_ = 2.0);

    ~BenchmarkResizableSkulkerHT();

    uint8_t Insert(uint64_t key, uint64_t value);
    uint64_t Query(uint64_t key, uint8_t ptr);
    void Update(uint64_t key, uint8_t ptr, uint64_t value);
    void Erase(uint64_t key, uint8_t ptr);

    void YCSBFill(std::vector<uint64_t>& keys, int num_threads);
    void YCSBRun(std::vector<std::pair<uint64_t, uint64_t>>& ops,
                 int num_threads);

    void ConcurrentRun(
        std::vector<std::tuple<uint64_t, uint64_t, uint64_t>>& ops,
        int num_threads);

   private:
    ResizableSkulkerHT* tab;
    uint64_t single_handle;
    int thread_num;
};

}  // namespace tinyptr
