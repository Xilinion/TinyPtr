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
    uint64_t gen_key_hittable();
    uint64_t gen_key_miss();
    uint64_t gen_value();
    // uint64_t gen_rand_64();

    int insert_cnt_to_overflow();
    uint64_t insert_delete_opt_to_overflow(uint64_t size, uint16_t bin_size,
                                           double opt_ratio,
                                           double load_factor);

    void obj_fill_vec_prepare(std::vector<uint64_t>& key_vec,
                              std::vector<uint64_t>& value_vec, int ins_cnt);
    void obj_fill(std::vector<uint64_t>& key_vec,
                  std::vector<uint64_t>& value_vec);
    void obj_fill_mem_free(int ins_cnt);

    void batch_query_vec_prepare(std::vector<uint64_t>& key_vec,
                                 std::vector<uint64_t>& query_key_vec,
                                 uint64_t op_cnt, double hit_rate);
    void batch_query(std::vector<uint64_t>& query_key_vec);
    void batch_query_no_mem(std::vector<uint64_t>& key_vec, int query_cnt,
                            double hit_rate);

    void batch_update_vec_prepare(std::vector<uint64_t>& key_vec,
                                  std::vector<uint64_t>& update_key_vec,
                                  std::vector<uint64_t>& update_value_vec,
                                  int update_cnt);
    void batch_update(std::vector<uint64_t>& update_key_vec,
                      std::vector<uint64_t>& update_value_vec);

    void erase_vec_prepare(std::vector<uint64_t>& key_vec,
                           std::vector<uint64_t>& erase_key_vec);
    void erase_all(std::vector<uint64_t>& erase_key_vec);

    void alternating_insert_erase(int opt_cnt);

    void all_operation_rand(std::vector<uint64_t>& key_vec, int opt_cnt);

    void ycsb_load(std::vector<uint64_t>& ycsb_keys, std::string path);
    void ycsb_exe_load(std::vector<std::pair<uint64_t, uint64_t>>& ycsb_exe_vec,
                       std::string path);

   public:
    void Run();

   private:
    rng::rng64 rgen64;
    rng::rng128 rgen128;

    bool rand_mem_free = false;

    std::ofstream output_stream;
    uint64_t table_size;
    uint64_t opt_num;
    uint64_t thread_num;
    int if_resize;
    double load_factor;
    double hit_ratio;

    BenchmarkObject64* obj;

    std::function<void()> run;
};
}  // namespace tinyptr