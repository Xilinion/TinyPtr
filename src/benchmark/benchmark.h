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

   private:
    struct RandVec {
        RandVec(uint64_t size);

        uint8_t gen_rand_nonzero_8();
        uint64_t gen_rand_odd();
        uint64_t gen_key_hittable();
        uint64_t gen_key_miss();
        uint64_t gen_value();

        rng::rng64 rgen64;
        rng::rng128 rgen128;

        uint64_t ptr_vec_head;
        std::vector<uint8_t> ptr_vec;
        uint64_t key_vec_head[2];
        std::vector<uint64_t> key_vec[2];
        uint64_t val_vec_head;
        std::vector<uint64_t> val_vec;
        uint64_t rand_vec_head;
        std::vector<uint64_t> rand_vec;
        std::vector<uint64_t> erase_order;
    }* rand_vec;

   public:
    Benchmark(BenchmarkCLIPara& para);
    ~Benchmark() = default;

   private:
    uint8_t gen_rand_nonzero_8();
    uint64_t gen_key_hittable();
    uint64_t gen_key_miss();
    uint64_t gen_value();
    uint64_t rgen64();
    void gen_rand_vector(uint64_t size);
    void gen_erase_order(uint64_t size);

    int insert_cnt_to_overflow();
    int insert_delete_opt_to_overflow(uint64_t size, uint16_t bin_size,
                                      double opt_ratio, double load_factor);
    void obj_fill(int ins_cnt);
    void batch_query_vec_prepare(std::vector<uint64_t>& query_key_vec,
                                 std::vector<uint8_t>& query_ptr_vec,
                                 uint64_t op_cnt, double hit_rate);
    void batch_query(std::vector<uint64_t>& query_key_vec,
                     std::vector<uint8_t>& query_ptr_vec, uint64_t query_cnt);
    void batch_query_no_mem(int query_cnt, double hit_rate);
    void batch_update(int update_cnt);
    void erase_all();
    void alternating_insert_erase(int opt_cnt);
    void all_operation_rand(int opt_cnt);

   public:
    void Run();

   private:
    bool rand_mem_free = false;

    std::ofstream output_stream;
    uint64_t table_size;
    uint64_t opt_num;
    double load_factor;
    double hit_ratio;

    BenchmarkObject64* obj;

    std::function<void()> run;
};
}  // namespace tinyptr