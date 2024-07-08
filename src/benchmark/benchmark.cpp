
#include "benchmark.h"
#include <algorithm>
#include <cmath>
#include <cstdint>
#include <ctime>
#include <cstdlib>
#include <iterator>
#include <ostream>
#include <random>
#include "../chained_ht_64.h"
#include "../dereference_table_64.h"
#include "benchmark_chainedht64.h"
#include "benchmark_cli_para.h"
#include "benchmark_dereftab64.h"
#include "benchmark_intarray64.h"
#include "benchmark_object_64.h"
#include "benchmark_std_unordered_map_64.h"

namespace tinyptr {

uint8_t Benchmark::gen_rand_nonzero_8() {
    uint8_t msk = ~(uint8_t(0));
    uint64_t res = rgen64();
    while (0 == (res & msk))
        res = rgen64();
    return (res & msk);
}

uint64_t Benchmark::gen_rand_odd() {
    // odd
    return rgen64() | 1;
}

uint64_t Benchmark::gen_key_hittable() {
    // odd
    return rgen64() | 1;
}

uint64_t Benchmark::gen_key_miss() {
    // even
    uint64_t tmp = rgen64();
    return tmp - (tmp & 1);
}

uint64_t Benchmark::gen_value() {
    return rgen64();
}

int Benchmark::insert_cnt_to_overflow() {
    for (int res = 0;; ++res) {
        if (~obj->Insert(gen_key_hittable(), gen_value()))
            return res;
    }
}

void Benchmark::obj_fill(int ins_cnt) {
    while (ins_cnt--) {
        uint64_t key = gen_key_hittable();
        key_vec.push_back(key);
        ptr_vec.push_back(obj->Insert(key, gen_value()));
    }
}

void Benchmark::batch_query(int query_cnt, double hit_rate) {
    uint32_t hit_bar;
    if (hit_rate > 1 - kEps) {
        hit_bar = ~(uint32_t(0));
    } else if (hit_rate < kEps) {
        hit_bar = 0;
    } else {
        hit_bar = uint32_t(floor(hit_rate * (~(uint32_t(0)))));
    }

    // we assume the all the elements in the vector are inserted
    // and nothing happens after that
    int key_ind_range = key_vec.size();

    while (query_cnt--) {
        uint64_t key;
        uint8_t ptr;

        if (hit_bar >= (gen_rand_odd() & (~(uint32_t(0))))) {
            int key_ind = rgen64() % key_ind_range;
            key = key_vec[key_ind];
            ptr = ptr_vec[key_ind];
        } else {
            key = gen_key_miss();
            ptr = gen_rand_nonzero_8();
        }

        obj->Query(key, ptr);
    }
}

void Benchmark::batch_update(int update_cnt) {

    // we assume the all the elements in the vector are inserted
    // and nothing happens after that
    int key_ind_range = key_vec.size();

    while (update_cnt--) {
        int key_ind = rgen64() % key_ind_range;
        uint64_t key = key_vec[key_ind];
        uint8_t ptr = ptr_vec[key_ind];
        obj->Update(key, ptr, gen_value());
    }
}

void Benchmark::erase_all() {
    int key_ind_range = key_vec.size();
    std::vector<int> erase_order(key_ind_range);
    for (int i = 0; i < key_ind_range; ++i)
        erase_order[i] = i;
    std::random_shuffle(erase_order.begin(), erase_order.end());

    // we assume the all the elements in the vector are inserted
    // and nothing happens after that
    for (auto iter : erase_order) {
        obj->Erase(key_vec[iter], ptr_vec[iter]);
    }
}

void Benchmark::alternating_insert_erase(int opt_cnt) {
    while (opt_cnt--) {
        uint64_t key = gen_key_hittable();
        obj->Erase(key, obj->Insert(key, gen_value()));
    }
}

void Benchmark::all_operation_rand(int opt_cnt) {
    while (opt_cnt--) {
        int key_ind_range = key_vec.size();
        int key_ind = rgen64() % key_ind_range;
        uint64_t key = key_vec[key_ind];
        uint8_t ptr = ptr_vec[key_ind];

        uint8_t opt_rand = gen_rand_nonzero_8();
        if (opt_rand <= 0b1111) {
            obj->Erase(key, ptr);
        } else if (opt_rand <= 0b111111) {
            key_vec.push_back(key);
            ptr_vec.push_back(obj->Insert(key, gen_value()));
        } else if (opt_rand <= 0b1111111) {
            obj->Update(key, ptr, gen_value());
        } else {
            obj->Query(key, ptr);
        }
    }
}

Benchmark::Benchmark(BenchmarkCLIPara& para)
    : output_stream(para.GetOuputFileName()),
      rgen64(rng::random_device_seed{}()),
      rgen128(rng::random_device_seed{}()),
      table_size(para.table_size),
      opt_num(para.opt_num),
      load_factor(para.load_factor),
      hit_ratio(para.hit_percent) {

    switch (para.object_id) {
        case BenchmarkObjectType::DEREFTAB64:
            obj = new BenchmarkDerefTab64(table_size);
            break;
        case BenchmarkObjectType::CHAINEDHT64:
            obj = new BenchmarkChainedHT64(table_size);
            break;
        case BenchmarkObjectType::INTARRAY64:
            obj = new BenchmarkIntArray64(table_size);
            break;
        case BenchmarkObjectType::STDUNORDEREDMAP64:
            obj = new BenchmarkStdUnorderedMap64(table_size);
            break;
        default:
            abort();
    }

    switch (para.case_id) {
        case BenchmarkCaseType::INSERT_ONLY_LOAD_FACTOR_SUPPORT:
            run = [this]() {
                std::clock_t start = std::clock();

                int load_cnt = insert_cnt_to_overflow();

                output_stream << "Table Size: " << table_size << std::endl;
                output_stream << "Load Capacity: " << load_cnt << std::endl;
                output_stream << "CPU Time: " << 1000.0 * (std::clock() - start) / CLOCKS_PER_SEC << std::endl;
            };
            break;
        case BenchmarkCaseType::INSERT_ONLY:
            run = [this]() {
                std::clock_t start = std::clock();
                
                obj_fill(opt_num);

                output_stream << "CPU Time: " << 1000.0 * (std::clock() - start) / CLOCKS_PER_SEC << std::endl;
            };
            break;
        case BenchmarkCaseType::UPDATE_ONLY:
            run = [this]() {
                obj_fill(table_size);

                std::clock_t start = std::clock();

                batch_update(opt_num);

                output_stream << "CPU Time: " << 1000.0 * (std::clock() - start) / CLOCKS_PER_SEC << std::endl;
            };
            break;
        case BenchmarkCaseType::ERASE_ONLY:
            run = [this]() {
                obj_fill(table_size);

                std::clock_t start = std::clock();

                erase_all();

                output_stream << "CPU Time: " << 1000.0 * (std::clock() - start) / CLOCKS_PER_SEC << std::endl;
            };
            break;
        case BenchmarkCaseType::ALTERNATING_INSERT_ERASE:
            run = [this]() {
                std::clock_t start = std::clock();

                alternating_insert_erase(opt_num);

                output_stream << "CPU Time: " << 1000.0 * (std::clock() - start) / CLOCKS_PER_SEC << std::endl;
            };
            break;
        case BenchmarkCaseType::QUERY_HIT_ONLY:
            run = [this]() {
                obj_fill(table_size);

                std::clock_t start = std::clock();

                batch_query(opt_num, 1);

                output_stream << "CPU Time: " << 1000.0 * (std::clock() - start) / CLOCKS_PER_SEC << std::endl;
            };
            break;
        case BenchmarkCaseType::QUERY_MISS_ONLY:
            run = [this]() {
                obj_fill(table_size);

                std::clock_t start = std::clock();

                batch_query(opt_num, 0);

                output_stream << "CPU Time: " << 1000.0 * (std::clock() - start) / CLOCKS_PER_SEC << std::endl;
            };
            break;
        case BenchmarkCaseType::QUERY_HIT_PERCENT:
            run = [this]() {
                obj_fill(table_size);

                std::clock_t start = std::clock();

                batch_query(opt_num, hit_ratio);

                output_stream << "CPU Time: " << 1000.0 * (std::clock() - start) / CLOCKS_PER_SEC << std::endl;
            };
            break;
        case BenchmarkCaseType::QUERY_HIT_ONLY_CUSTOM_LOAD_FACTOR:
            run = [this]() {
                obj_fill(int(floor(table_size * load_factor)));

                std::clock_t start = std::clock();

                batch_query(opt_num, 1);

                output_stream << "CPU Time: " << 1000.0 * (std::clock() - start) / CLOCKS_PER_SEC << std::endl;
            };
            break;
        case BenchmarkCaseType::QUERY_MISS_ONLY_CUSTOM_LOAD_FACTOR:
            run = [this]() {
                obj_fill(int(floor(table_size * load_factor)));

                std::clock_t start = std::clock();

                batch_query(opt_num, 0);

                output_stream << "CPU Time: " << 1000.0 * (std::clock() - start) / CLOCKS_PER_SEC << std::endl;
            };
            break;
        case BenchmarkCaseType::QUERY_HIT_PERCENT_CUSTOM_LOAD_FACTOR:
            run = [this]() {
                obj_fill(int(floor(table_size * load_factor)));

                std::clock_t start = std::clock();

                batch_query(opt_num, hit_ratio);

                output_stream << "CPU Time: " << 1000.0 * (std::clock() - start) / CLOCKS_PER_SEC << std::endl;
            };
            break;
        case BenchmarkCaseType::ALL_OPERATION_RAND:
            run = [this]() {
                std::clock_t start = std::clock();

                all_operation_rand(opt_num);

                output_stream << "CPU Time: " << 1000.0 * (std::clock() - start) / CLOCKS_PER_SEC << std::endl;
            };
            break;
        default:
            abort();
    }
}

void Benchmark::Run() {
    this->run();
}
}  // namespace tinyptr