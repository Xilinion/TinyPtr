#pragma once

#include <sys/types.h>
#include <csignal>
#include <cstdint>
#include <fstream>
#include <functional>
#include <iostream>
#include <string>
#include <vector>
#include "../utils/rng.h"
#include "benchmark_case_type.h"
#include "benchmark_cli_para.h"
#include "benchmark_object_64.h"
#include "benchmark_object_type.h"

namespace tinyptr {

class Benchmark {
   public:
    static constexpr double kEps = 1e-6;

   public:
    Benchmark(BenchmarkCLIPara& para);
    ~Benchmark() = default;

   private:
    uint8_t gen_rand_nonzero_8();
    uint64_t gen_rand_odd();
    uint64_t gen_key_hittable();
    uint64_t gen_key_miss();
    uint64_t gen_value();

    int insert_cnt_to_overflow();
    void obj_fill(int ins_cnt);
    void batch_query(int query_cnt, double hit_rate);
    void batch_update(int update_cnt);
    void erase_all();
    void alternating_insert_erase(int opt_cnt);
    void all_operation_rand(int opt_cnt);

   public:
    void Run();

   private:
    std::ofstream output_stream;
    int table_size;
    uint64_t opt_num;
    double load_factor;
    double hit_ratio;

    BenchmarkObject64* obj;
    std::vector<uint8_t> ptr_vec;
    std::vector<uint64_t> key_vec;

    std::function<void()> run;

    rng::rng64 rgen64;
    rng::rng128 rgen128;
};
}  // namespace tinyptr