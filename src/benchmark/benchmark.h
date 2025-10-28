#pragma once

#include <sys/types.h>
#include <csignal>
#include <cstdint>
#include <fstream>
#include <functional>
#include <iostream>
#include <string>
#include <vector>
#include "benchmark_case_type.h"
#include "benchmark_cli_para.h"
#include "benchmark_object_64.h"
#include "benchmark_object_type.h"
#include "blast_ht.h"
#include "utils/rng.h"

namespace tinyptr {

class Benchmark {
   public:
    static constexpr double kEps = 1e-6;

   private:
    class ZipfianGenerator {
       public:
        ZipfianGenerator(int n, double skew)
            : n_(n), gen_(std::random_device{}()) {
            lookup_.resize(n_);

            double sum = 0.0;
            for (int i = 1; i <= n_; i++) {
                sum += 1.0 / std::pow(i, skew);
            }

            double cumulative = 0.0;
            for (int i = 1; i <= n_; i++) {
                double p = (1.0 / std::pow(i, skew)) / sum;
                cumulative += p;
                int idx_end = static_cast<int>(cumulative * n_);
                for (int j =
                         (i == 1 ? 0 : static_cast<int>((cumulative - p) * n_));
                     j < idx_end; j++) {
                    lookup_[j] = i - 1;
                }
            }
            lookup_[n_ - 1] = n_ - 1;
        }

        int next() { return lookup_[gen_() % n_]; }

       private:
        int n_;
        std::mt19937 gen_;
        std::vector<int> lookup_;
    };

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

    void ycsb_load(std::vector<uint64_t>& ycsb_keys, std::string path,
                   double load_factor);
    void ycsb_exe_load(std::vector<std::pair<uint64_t, uint64_t>>& ycsb_exe_vec,
                       std::string path, double load_factor,
                       BlastHT* available_keys);

    void vec_to_ops(std::vector<uint64_t>& key_vec,
                    std::vector<std::tuple<uint64_t, uint64_t, uint64_t>>& ops,
                    uint64_t opt_type);

    void vec_to_ops(std::vector<uint64_t>& key_vec,
                    std::vector<uint64_t>& value_vec,
                    std::vector<std::tuple<uint64_t, uint64_t, uint64_t>>& ops,
                    uint64_t opt_type);

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
    double load_factor;
    double hit_ratio;
    double zipfian_skew;

    BenchmarkObject64* obj;

    std::function<void()> run;
};
}  // namespace tinyptr