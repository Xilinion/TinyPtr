
#include "benchmark.h"
#include <algorithm>
#include <cmath>
#include <cstdint>
#include <cstdlib>
#include <ctime>
#include <iterator>
#include <ostream>
#include <random>
#include "../chained_ht_64.h"
#include "../dereference_table_64.h"
#include "benchmark_bytearray_chainedht.h"
#include "benchmark_chainedht64.h"
#include "benchmark_clht.h"
#include "benchmark_cli_para.h"
#include "benchmark_cuckoo.h"
#include "benchmark_dereftab64.h"
#include "benchmark_iceberg.h"
#include "benchmark_intarray64.h"
#include "benchmark_object_64.h"
#include "benchmark_std_unordered_map_64.h"

namespace tinyptr {

Benchmark::RandVec::RandVec(uint64_t size)
    : ptr_vec_head(0),
      ptr_vec((size << 2)),
      key_vec_head{0, 0},
      key_vec{std::vector<uint64_t>(size << 2),
              std::vector<uint64_t>(size << 2)},
      val_vec(size << 2),
      rgen64(rng::random_device_seed{}()),
      rgen128(rng::random_device_seed{}()),
      rand_vec_head(0),
      rand_vec(size << 2) {

    size <<= 2;
    for (uint64_t i = 0; i < size; ++i) {
        ptr_vec[i] = gen_rand_nonzero_8();
        key_vec[0][i] = gen_key_hittable();
        key_vec[1][i] = gen_key_miss();
        val_vec[i] = gen_value();
        rand_vec[i] = gen_value();
    }
}

uint8_t Benchmark::RandVec::gen_rand_nonzero_8() {
    uint8_t msk = ~(uint8_t(0));
    uint64_t res = rgen64();
    while (0 == (res & msk))
        res = rgen64();
    return (res & msk);
}

uint8_t Benchmark::gen_rand_nonzero_8() {
    return rand_vec->ptr_vec[rand_vec->ptr_vec_head++];
}

uint64_t Benchmark::RandVec::gen_rand_odd() {
    // odd
    return rgen64() | 1;
}

uint64_t Benchmark::RandVec::gen_key_hittable() {
    // odd
    return rgen64() | 1;
}

uint64_t Benchmark::gen_key_hittable() {
    // odd
    return rand_vec->key_vec[0][rand_vec->key_vec_head[0]++];
}

uint64_t Benchmark::RandVec::gen_key_miss() {
    // even
    uint64_t tmp = rgen64();
    return tmp - (tmp & 1);
}

uint64_t Benchmark::gen_key_miss() {
    // even
    return rand_vec->key_vec[1][rand_vec->key_vec_head[1]++];
}

uint64_t Benchmark::rgen64() {
    return rand_vec->rand_vec[rand_vec->rand_vec_head++];
}

uint64_t Benchmark::RandVec::gen_value() {
    return rgen64();
}

uint64_t Benchmark::gen_value() {
    return rand_vec->val_vec[rand_vec->val_vec_head++];
}

void Benchmark::gen_rand_vector(uint64_t size) {
    rand_vec = new RandVec(size);
}

void Benchmark::gen_erase_order(uint64_t size) {
    int key_ind_range = table_size;

    rand_vec->erase_order.resize(key_ind_range);
    for (int i = 0; i < key_ind_range; ++i)
        rand_vec->erase_order[i] = i;
    std::random_shuffle(rand_vec->erase_order.begin(),
                        rand_vec->erase_order.end());
}

int Benchmark::insert_cnt_to_overflow() {
    for (int res = 0;; ++res) {
        if (!((uint8_t)(~obj->Insert(gen_key_hittable(), gen_value()))))
            return res;
    }
}

void Benchmark::obj_fill(int ins_cnt) {
    while (ins_cnt--) {
        obj->Insert(gen_key_hittable(), gen_value());
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
    int key_ind_range = table_size;

    while (query_cnt--) {
        uint64_t key;
        uint8_t ptr;

        if (hit_bar >= (gen_key_hittable() & (~(uint32_t(0))))) {
            int key_ind = rgen64() % key_ind_range;
            key = rand_vec->key_vec[0][key_ind];
            ptr = rand_vec->ptr_vec[key_ind];
        } else {
            key = gen_key_miss();
            ptr = gen_rand_nonzero_8();
        }

        obj->Query(key, ptr);
    }
}

void Benchmark::batch_query_no_mem(int query_cnt, double hit_rate) {
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
    int key_ind_range = table_size;

    while (query_cnt--) {
        uint64_t key;
        uint8_t ptr;

        if (hit_bar >= (gen_key_hittable() & (~(uint32_t(0))))) {
            int key_ind = rgen64() % key_ind_range;
            key = rand_vec->key_vec[0][key_ind];
            ptr = rand_vec->ptr_vec[key_ind];
        } else {
            key = gen_key_miss();
            ptr = gen_rand_nonzero_8();
        }

        uint64_t value;
        dynamic_cast<BenchmarkByteArrayChained*>(obj)->QueryNoMem(key, &value);
    }
}

void Benchmark::batch_update(int update_cnt) {

    // we assume the all the elements in the vector are inserted
    // and nothing happens after that
    int key_ind_range = table_size;

    while (update_cnt--) {
        int key_ind = rgen64() % key_ind_range;
        uint64_t key = rand_vec->key_vec[0][key_ind];
        uint8_t ptr = rand_vec->ptr_vec[key_ind];
        obj->Update(key, ptr, gen_value());
    }
}

void Benchmark::erase_all() {
    // we assume the all the elements in the vector are inserted
    // and nothing happens after that
    for (auto iter : rand_vec->erase_order) {
        obj->Erase(rand_vec->key_vec[0][iter], rand_vec->ptr_vec[iter]);
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
        int key_ind_range = table_size;
        int key_ind = rgen64() % key_ind_range;
        uint64_t key = rand_vec->key_vec[0][key_ind];
        uint8_t ptr = rand_vec->ptr_vec[key_ind];

        uint8_t opt_rand = gen_rand_nonzero_8();
        if (opt_rand <= 0b1111) {
            obj->Erase(key, ptr);
        } else if (opt_rand <= 0b111111) {
            obj->Insert(key, gen_value());
        } else if (opt_rand <= 0b1111111) {
            obj->Update(key, ptr, gen_value());
        } else {
            obj->Query(key, ptr);
        }
    }
}

Benchmark::Benchmark(BenchmarkCLIPara& para)
    : output_stream(para.GetOuputFileName()),
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
        case BenchmarkObjectType::BYTEARRAYCHAINEDHT:
            obj = new BenchmarkByteArrayChained(table_size * 1.031,
                                                para.quotienting_tail_length,
                                                para.bin_size);
            break;
        case BenchmarkObjectType::CLHT:
            obj = new BenchmarkCLHT(table_size);
            break;
        case BenchmarkObjectType::CUCKOO:
            obj = new BenchmarkCuckoo(table_size);
            break;
        case BenchmarkObjectType::ICEBERG:
            obj = new BenchmarkIceberg(table_size);
            break;
        default:
            abort();
    }

    gen_rand_vector(std::max(table_size, opt_num));

    switch (para.case_id) {
        case BenchmarkCaseType::INSERT_ONLY_LOAD_FACTOR_SUPPORT:
            run = [this, para]() {
                if (para.object_id == BenchmarkObjectType::BYTEARRAYCHAINEDHT) {
                    obj = new BenchmarkByteArrayChained(
                        table_size, para.quotienting_tail_length,
                        para.bin_size);
                }
                std::clock_t start = std::clock();

                int load_cnt = insert_cnt_to_overflow();

                auto end = std::clock();
                auto duration = end - start;

                output_stream << "Table Size: " << table_size << std::endl;
                output_stream << "Load Capacity: " << load_cnt << std::endl;
                output_stream
                    << "CPU Time: "
                    << int(1000.0 * (std::clock() - start) / CLOCKS_PER_SEC)
                    << " ms" << std::endl;

                output_stream << "Load Factor: "
                              << double(load_cnt) / double(table_size) * 100
                              << " %" << std::endl;

                output_stream
                    << "Throughput: "
                    << int(double(load_cnt) / double(duration) * CLOCKS_PER_SEC)
                    << " ops/s" << std::endl;

                output_stream
                    << "Latency: "
                    << int(double(duration) / double(load_cnt) /
                           CLOCKS_PER_SEC * (1ll * 1000 * 1000 * 1000))
                    << " ns/op" << std::endl;

                if (para.object_id == BenchmarkObjectType::BYTEARRAYCHAINEDHT) {
                    output_stream
                        << "Avg Chain Length: "
                        << dynamic_cast<BenchmarkByteArrayChained*>(obj)
                               ->AvgChainLength()
                        << std::endl;

                    uint32_t max_chain_length;

                    output_stream
                        << "Max Chain Length: "
                        << (max_chain_length =
                                dynamic_cast<BenchmarkByteArrayChained*>(obj)
                                    ->MaxChainLength())
                        << std::endl;

                    auto hist = dynamic_cast<BenchmarkByteArrayChained*>(obj)
                                    ->ChainLengthHistogram();
                    output_stream << "Chain Length Histogram: " << std::endl;
                    output_stream << "\tLength\t\tCount" << std::endl;
                    for (uint32_t i = 0; i <= max_chain_length; ++i) {
                        output_stream << "\t" << i << "\t\t" << hist[i]
                                      << std::endl;
                    }
                }
            };
            break;
        case BenchmarkCaseType::INSERT_ONLY:
            run = [this]() {
                std::clock_t start = std::clock();

                obj_fill(opt_num);

                auto end = std::clock();
                auto duration = end - start;

                output_stream
                    << "CPU Time: "
                    << int(1000.0 * (std::clock() - start) / CLOCKS_PER_SEC)
                    << std::endl;

                output_stream
                    << "Throughput: "
                    << int(double(opt_num) / double(duration) * CLOCKS_PER_SEC)
                    << " ops/s" << std::endl;

                output_stream
                    << "Latency: "
                    << int(double(duration) / double(opt_num) / CLOCKS_PER_SEC *
                           (1ll * 1000 * 1000 * 1000))
                    << " ns/op" << std::endl;
            };
            break;
        case BenchmarkCaseType::UPDATE_ONLY:
            run = [this]() {
                obj_fill(table_size);

                std::clock_t start = std::clock();

                batch_update(opt_num);

                auto end = std::clock();
                auto duration = end - start;

                output_stream
                    << "CPU Time: "
                    << int(1000.0 * (std::clock() - start) / CLOCKS_PER_SEC)
                    << std::endl;

                output_stream
                    << "Throughput: "
                    << int(double(opt_num) / double(duration) * CLOCKS_PER_SEC)
                    << " ops/s" << std::endl;

                output_stream
                    << "Latency: "
                    << int(double(duration) / double(opt_num) / CLOCKS_PER_SEC *
                           (1ll * 1000 * 1000 * 1000))
                    << " ns/op" << std::endl;
            };
            break;
        case BenchmarkCaseType::ERASE_ONLY:
            run = [this]() {
                obj_fill(table_size);
                gen_erase_order(table_size);

                std::clock_t start = std::clock();

                erase_all();

                auto end = std::clock();
                auto duration = end - start;

                output_stream
                    << "CPU Time: "
                    << int(1000.0 * (std::clock() - start) / CLOCKS_PER_SEC)
                    << std::endl;

                output_stream
                    << "Throughput: "
                    << int(double(opt_num) / double(duration) * CLOCKS_PER_SEC)
                    << " ops/s" << std::endl;

                output_stream
                    << "Latency: "
                    << int(double(duration) / double(opt_num) / CLOCKS_PER_SEC *
                           (1ll * 1000 * 1000 * 1000))
                    << " ns/op" << std::endl;
            };
            break;
        case BenchmarkCaseType::ALTERNATING_INSERT_ERASE:
            run = [this]() {
                std::clock_t start = std::clock();

                alternating_insert_erase(opt_num);

                auto end = std::clock();
                auto duration = end - start;

                output_stream
                    << "CPU Time: "
                    << int(1000.0 * (std::clock() - start) / CLOCKS_PER_SEC)
                    << std::endl;

                output_stream
                    << "Throughput: "
                    << int(double(opt_num) / double(duration) * CLOCKS_PER_SEC)
                    << " ops/s" << std::endl;

                output_stream
                    << "Latency: "
                    << int(double(duration) / double(opt_num) / CLOCKS_PER_SEC *
                           (1ll * 1000 * 1000 * 1000))
                    << " ns/op" << std::endl;
            };
            break;
        case BenchmarkCaseType::QUERY_HIT_ONLY:
            run = [this]() {
                obj_fill(table_size);

                std::clock_t start = std::clock();

                batch_query(opt_num, 1);

                auto end = std::clock();
                auto duration = end - start;

                output_stream
                    << "CPU Time: "
                    << int(1000.0 * (std::clock() - start) / CLOCKS_PER_SEC)
                    << std::endl;

                output_stream
                    << "Throughput: "
                    << int(double(opt_num) / double(duration) * CLOCKS_PER_SEC)
                    << " ops/s" << std::endl;

                output_stream
                    << "Latency: "
                    << int(double(duration) / double(opt_num) / CLOCKS_PER_SEC *
                           (1ll * 1000 * 1000 * 1000))
                    << " ns/op" << std::endl;
            };
            break;
        case BenchmarkCaseType::QUERY_MISS_ONLY:
            run = [this]() {
                obj_fill(table_size);

                std::clock_t start = std::clock();

                batch_query(opt_num, 0);

                auto end = std::clock();
                auto duration = end - start;

                output_stream
                    << "CPU Time: "
                    << int(1000.0 * (std::clock() - start) / CLOCKS_PER_SEC)
                    << std::endl;

                output_stream
                    << "Throughput: "
                    << int(double(opt_num) / double(duration) * CLOCKS_PER_SEC)
                    << " ops/s" << std::endl;

                output_stream
                    << "Latency: "
                    << int(double(duration) / double(opt_num) / CLOCKS_PER_SEC *
                           (1ll * 1000 * 1000 * 1000))
                    << " ns/op" << std::endl;
            };
            break;
        case BenchmarkCaseType::QUERY_HIT_PERCENT:
            run = [this]() {
                obj_fill(table_size);

                std::clock_t start = std::clock();

                batch_query(opt_num, hit_ratio);

                auto end = std::clock();
                auto duration = end - start;

                output_stream
                    << "CPU Time: "
                    << int(1000.0 * (std::clock() - start) / CLOCKS_PER_SEC)
                    << std::endl;

                output_stream
                    << "Throughput: "
                    << int(double(opt_num) / double(duration) * CLOCKS_PER_SEC)
                    << " ops/s" << std::endl;

                output_stream
                    << "Latency: "
                    << int(double(duration) / double(opt_num) / CLOCKS_PER_SEC *
                           (1ll * 1000 * 1000 * 1000))
                    << " ns/op" << std::endl;
            };
            break;
        case BenchmarkCaseType::QUERY_HIT_ONLY_CUSTOM_LOAD_FACTOR:
            run = [this]() {
                obj_fill(int(floor(table_size * load_factor)));

                std::clock_t start = std::clock();

                batch_query(opt_num, 1);

                auto end = std::clock();
                auto duration = end - start;

                output_stream
                    << "CPU Time: "
                    << int(1000.0 * (std::clock() - start) / CLOCKS_PER_SEC)
                    << std::endl;

                output_stream
                    << "Throughput: "
                    << int(double(opt_num) / double(duration) * CLOCKS_PER_SEC)
                    << " ops/s" << std::endl;

                output_stream
                    << "Latency: "
                    << int(double(duration) / double(opt_num) / CLOCKS_PER_SEC *
                           (1ll * 1000 * 1000 * 1000))
                    << " ns/op" << std::endl;
            };
            break;
        case BenchmarkCaseType::QUERY_MISS_ONLY_CUSTOM_LOAD_FACTOR:
            run = [this]() {
                obj_fill(int(floor(table_size * load_factor)));

                std::clock_t start = std::clock();

                batch_query(opt_num, 0);

                auto end = std::clock();
                auto duration = end - start;

                output_stream
                    << "CPU Time: "
                    << int(1000.0 * (std::clock() - start) / CLOCKS_PER_SEC)
                    << std::endl;

                output_stream
                    << "Throughput: "
                    << int(double(opt_num) / double(duration) * CLOCKS_PER_SEC)
                    << " ops/s" << std::endl;

                output_stream
                    << "Latency: "
                    << int(double(duration) / double(opt_num) / CLOCKS_PER_SEC *
                           (1ll * 1000 * 1000 * 1000))
                    << " ns/op" << std::endl;
            };
            break;
        case BenchmarkCaseType::QUERY_HIT_PERCENT_CUSTOM_LOAD_FACTOR:
            run = [this]() {
                obj_fill(int(floor(table_size * load_factor)));

                std::clock_t start = std::clock();

                batch_query(opt_num, hit_ratio);

                auto end = std::clock();
                auto duration = end - start;

                output_stream
                    << "CPU Time: "
                    << int(1000.0 * (std::clock() - start) / CLOCKS_PER_SEC)
                    << std::endl;

                output_stream
                    << "Throughput: "
                    << int(double(opt_num) / double(duration) * CLOCKS_PER_SEC)
                    << " ops/s" << std::endl;

                output_stream
                    << "Latency: "
                    << int(double(duration) / double(opt_num) / CLOCKS_PER_SEC *
                           (1ll * 1000 * 1000 * 1000))
                    << " ns/op" << std::endl;
            };
            break;
        case BenchmarkCaseType::ALL_OPERATION_RAND:
            run = [this]() {
                std::clock_t start = std::clock();

                all_operation_rand(opt_num);

                auto end = std::clock();
                auto duration = end - start;

                output_stream
                    << "CPU Time: "
                    << int(1000.0 * (std::clock() - start) / CLOCKS_PER_SEC)
                    << std::endl;

                output_stream
                    << "Throughput: "
                    << int(double(opt_num) / double(duration) * CLOCKS_PER_SEC)
                    << " ops/s" << std::endl;

                output_stream
                    << "Latency: "
                    << int(double(duration) / double(opt_num) / CLOCKS_PER_SEC *
                           (1ll * 1000 * 1000 * 1000))
                    << " ns/op" << std::endl;
            };
            break;
        case BenchmarkCaseType::XXHASH64_THROUGHPUT:
            obj = new BenchmarkIntArray64(1);
            run = [this]() {
                std::clock_t start = std::clock();

                auto tmp = rgen64();
                for (int i = 0; i < opt_num; ++i) {
                    // SlowXXHash64::hash(&tmp, sizeof(uint64_t), 233);
                    XXH64(&tmp, sizeof(uint64_t), 233);
                }

                auto end = std::clock();
                auto duration = end - start;

                output_stream
                    << "CPU Time: "
                    << int(1000.0 * (std::clock() - start) / CLOCKS_PER_SEC)
                    << std::endl;

                output_stream
                    << "Throughput: "
                    << int(double(opt_num) / double(duration) * CLOCKS_PER_SEC)
                    << " ops/s" << std::endl;

                output_stream
                    << "Latency: "
                    << int(double(duration) / double(opt_num) / CLOCKS_PER_SEC *
                           (1ll * 1000 * 1000 * 1000))
                    << " ns/op" << std::endl;
            };
            break;
        case BenchmarkCaseType::PRNG_THROUGHPUT:
            obj = new BenchmarkIntArray64(1);
            run = [this]() {
                std::clock_t start = std::clock();

                auto tmp = rgen64();
                for (int i = 0; i < opt_num; ++i) {
                    tmp = rgen64();
                }

                auto end = std::clock();
                auto duration = end - start;

                output_stream
                    << "CPU Time: "
                    << int(1000.0 * (std::clock() - start) / CLOCKS_PER_SEC)
                    << std::endl;

                output_stream
                    << "Throughput: "
                    << int(double(opt_num) / double(duration) * CLOCKS_PER_SEC)
                    << " ops/s" << std::endl;

                output_stream
                    << "Latency: "
                    << int(double(duration) / double(opt_num) / CLOCKS_PER_SEC *
                           (1ll * 1000 * 1000 * 1000))
                    << " ns/op" << std::endl;
            };
            break;
        case BenchmarkCaseType::LATENCY_VARYING_CHAINLENGTH:
            run = [this, para]() {
                uint8_t quot_length = 16;
                uint64_t case_table_size = table_size >> 16;
                while (case_table_size) {
                    case_table_size >>= 1;
                    quot_length++;
                }
                case_table_size = 1 << quot_length;

                for (uint8_t chain_length = 0; chain_length < 16;
                     chain_length = chain_length ? chain_length <<= 1 : 1,
                             quot_length--) {
                    obj = new BenchmarkByteArrayChained(
                        case_table_size * 1.031, quot_length, para.bin_size);
                    dynamic_cast<BenchmarkByteArrayChained*>(obj)
                        ->FillChainLength(chain_length);

                    gen_rand_vector(case_table_size);

                    std::clock_t start = std::clock();

                    batch_query(opt_num, 0);

                    auto end = std::clock();
                    auto duration = end - start;

                    output_stream << "Table Size: " << case_table_size
                                  << std::endl;
                    output_stream << "Chain Length: " << int(chain_length)
                                  << std::endl;

                    output_stream
                        << "CPU Time: "
                        << int(1000.0 * (std::clock() - start) / CLOCKS_PER_SEC)
                        << std::endl;

                    output_stream << "Throughput: "
                                  << int(double(opt_num) / double(duration) *
                                         CLOCKS_PER_SEC)
                                  << " ops/s" << std::endl;

                    output_stream
                        << "Latency: "
                        << int(double(duration) / double(opt_num) /
                               CLOCKS_PER_SEC * (1ll * 1000 * 1000 * 1000))
                        << " ns/op" << std::endl;

                    output_stream << std::endl;
                }
            };
            break;
        case BenchmarkCaseType::QUERY_NO_MEM:
            run = [this, para]() {
                uint8_t quot_length = 16;
                uint64_t case_table_size = table_size >> 16;
                while (case_table_size) {
                    case_table_size >>= 1;
                    quot_length++;
                }
                case_table_size = 1 << quot_length;

                for (uint8_t chain_length = 0; chain_length < 16;
                     chain_length = chain_length ? chain_length <<= 1 : 1,
                             quot_length--) {
                    obj = new BenchmarkByteArrayChained(
                        case_table_size * 1.031, quot_length, para.bin_size);
                    dynamic_cast<BenchmarkByteArrayChained*>(obj)
                        ->FillChainLength(chain_length);

                    dynamic_cast<BenchmarkByteArrayChained*>(obj)->set_chain_length(chain_length);

                    gen_rand_vector(case_table_size);

                    std::clock_t start = std::clock();

                    batch_query_no_mem(opt_num, 1);
                    // batch_query_no_mem(opt_num, 0);

                    auto end = std::clock();
                    auto duration = end - start;

                    output_stream << "Query No Mem" << std::endl;
                    output_stream << "Table Size: " << case_table_size << std::endl;
                    output_stream << "Chain Length: " << int(chain_length)
                                  << std::endl;

                    output_stream
                        << "CPU Time: "
                        << int(1000.0 * (std::clock() - start) / CLOCKS_PER_SEC)
                        << std::endl;

                    output_stream << "Throughput: "
                                  << int(double(opt_num) / double(duration) *
                                         CLOCKS_PER_SEC)
                                  << " ops/s" << std::endl;

                    output_stream
                        << "Latency: "
                        << int(double(duration) / double(opt_num) /
                               CLOCKS_PER_SEC * (1ll * 1000 * 1000 * 1000))
                        << " ns/op" << std::endl;

                    output_stream << std::endl;
                }
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
