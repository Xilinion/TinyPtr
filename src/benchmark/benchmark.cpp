
#include "benchmark.h"
#include <unistd.h>
#include <algorithm>
#include <cmath>
#include <cstdint>
#include <cstdlib>
#include <ctime>
#include <iostream>
#include <iterator>
#include <ostream>
#include <queue>
#include <random>
#include <set>
#include <unordered_set>
#include "../chained_ht_64.h"
#include "../dereference_table_64.h"
#include "benchmark_bin_aware_chainedht.h"
#include "benchmark_bytearray_chainedht.h"
#include "benchmark_chained.h"
#include "benchmark_chainedht64.h"
#include "benchmark_clht.h"
#include "benchmark_cli_para.h"
#include "benchmark_cuckoo.h"
#include "benchmark_dereftab64.h"
#include "benchmark_growt.h"
#include "benchmark_iceberg.h"
#include "benchmark_intarray64.h"
#include "benchmark_object_64.h"
#include "benchmark_same_bin_chainedht.h"
#include "benchmark_skulkerht.h"
#include "benchmark_std_unordered_map_64.h"
#include "benchmark_yarded_tp_ht.h"

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
    if (rand_mem_free) {
        return rand_vec->gen_rand_nonzero_8();
    }
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
    if (rand_mem_free) {
        return rand_vec->gen_key_hittable();
    }
    return rand_vec->key_vec[0][rand_vec->key_vec_head[0]++];
}

uint64_t Benchmark::RandVec::gen_key_miss() {
    // even
    uint64_t tmp = rgen64();
    return tmp - (tmp & 1);
}

uint64_t Benchmark::gen_key_miss() {
    // even
    if (rand_mem_free) {
        return rand_vec->gen_key_miss();
    }
    return rand_vec->key_vec[1][rand_vec->key_vec_head[1]++];
}

uint64_t Benchmark::rgen64() {
    if (rand_mem_free) {
        return rand_vec->rgen64();
    }

    if (rand_vec->rand_vec_head >= rand_vec->rand_vec.size()) {
        rand_vec->rand_vec_head = 0;
        int tmp = rand_vec->rand_vec.size();
        for (int i = 0; i < tmp; ++i) {
            rand_vec->rand_vec[i] = rand_vec->gen_value();
        }
    }
    return rand_vec->rand_vec[rand_vec->rand_vec_head++];
}

uint64_t Benchmark::RandVec::gen_value() {
    return rgen64();
}

uint64_t Benchmark::gen_value() {
    if (rand_mem_free) {
        return rgen64();
    }
    return rand_vec->val_vec[rand_vec->val_vec_head++];
}

void Benchmark::gen_rand_vector(uint64_t size) {
    if (rand_mem_free) {
        return;
    }
    rand_vec = new RandVec(size);
}

void Benchmark::gen_erase_order(uint64_t size) {
    if (rand_mem_free) {
        return;
    }
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

uint64_t Benchmark::insert_delete_opt_to_overflow(uint64_t size,
                                                  uint16_t bin_size,
                                                  double opt_ratio,
                                                  double load_factor) {
    size = (size + bin_size - 1) / bin_size * bin_size;
    int mx_cnt = int(floor(size * load_factor));

    std::vector<uint64_t> inserted_keys_vec;
    std::queue<uint64_t> vac_pos_queue;

    auto dynamic_cast_obj = dynamic_cast<BenchmarkByteArrayChained*>(obj);

    uint64_t val_tmp = 0;

    for (int i = 0; i < mx_cnt; ++i) {
        uint64_t key = gen_key_hittable();
        while (dynamic_cast_obj->Query(key, &val_tmp) || key == 0) {
            key = gen_key_hittable();
        }
        dynamic_cast_obj->Insert(key, i);
        inserted_keys_vec.push_back(key);
    }

    uint64_t opt_stop_cnt = 1ll * size * 100;
    uint64_t operation_count = 0;

    while (operation_count < opt_stop_cnt) {

        if (rgen64() % 2) {
            int num_inserts = rgen64() % int(size * opt_ratio) + 1;
            for (int i = 0; i < num_inserts; ++i) {
                if (vac_pos_queue.empty()) {
                    break;
                }
                uint64_t pos = vac_pos_queue.front();
                vac_pos_queue.pop();

                uint64_t key;
                do {
                    key = rgen64();
                } while (dynamic_cast_obj->Query(key, &val_tmp) || key == 0);

                // printf("Inserting key: %llu\n", key);

                if (!((uint8_t)(~dynamic_cast_obj->Insert(key, pos)))) {
                    return operation_count;
                }
                inserted_keys_vec[pos] = key;
                operation_count++;
            }
        } else {

            int num_deletes = rgen64() % int(size * opt_ratio) + 1;
            for (int i = 0; i < num_deletes; ++i) {
                if (inserted_keys_vec.empty()) {
                    continue;
                }

                uint64_t pos = rgen64() % mx_cnt;

                while (inserted_keys_vec[pos] == 0) {
                    pos = rgen64() % mx_cnt;
                }

                uint64_t key = inserted_keys_vec[pos];
                inserted_keys_vec[pos] = 0;

                // continue;
                dynamic_cast_obj->Erase(
                    key,
                    rgen64());  // Assuming Erase takes two arguments
                vac_pos_queue.push(pos);
                operation_count++;
            }
        }
    }

    if (operation_count >= opt_stop_cnt) {
        std::cerr << "good run!" << std::endl;
    }
    return operation_count;
}

void Benchmark::obj_fill(int ins_cnt) {
    while (ins_cnt--) {
        obj->Insert(gen_key_hittable(), gen_value());
    }
}

void Benchmark::batch_query_vec_prepare(std::vector<uint64_t>& query_key_vec,
                                        std::vector<uint8_t>& query_ptr_vec,
                                        uint64_t op_cnt, double hit_rate) {
    query_key_vec.resize(op_cnt);
    query_ptr_vec.resize(op_cnt);

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

    while (op_cnt--) {
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

        query_key_vec[op_cnt] = key;
        query_ptr_vec[op_cnt] = ptr;
    }
}

void Benchmark::batch_query(std::vector<uint64_t>& query_key_vec,
                            std::vector<uint8_t>& query_ptr_vec,
                            uint64_t query_cnt) {
    for (uint64_t i = 0; i < query_cnt; ++i) {
        obj->Query(query_key_vec[i], query_ptr_vec[i]);
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

void Benchmark::ycsb_load(std::vector<uint64_t>& ycsb_keys, std::string path) {
    FILE* file = fopen(path.c_str(), "r");
    if (file) {
        char buffer[256];
        while (fgets(buffer, sizeof(buffer), file)) {
            sscanf(buffer + 7, "%llu", &key);
            ycsb_keys.push_back(key);
        }
        fclose(file);
    }
}

void Benchmark::ycsb_exe_load(
    std::vector<std::pair<uint64_t, uint64_t>>& ycsb_exe_vec,
    std::string path) {
    FILE* file = fopen(path.c_str(), "r");
    if (file) {
        char buffer[256];
        while (fgets(buffer, sizeof(buffer), file)) {
            uint64_t key;
            if (strncmp(buffer, "INSERT", 6) == 0) {
                sscanf(buffer + 7, "%llu", &key);
                ycsb_exe_vec.emplace_back(1, key);
            } else if (strncmp(buffer, "READ", 4) == 0) {
                sscanf(buffer + 5, "%llu", &key);
                ycsb_exe_vec.emplace_back(0, key);
            }
        }
        fclose(file);
    }
}

Benchmark::Benchmark(BenchmarkCLIPara& para)
    : output_stream(para.GetOuputFileName()),
      table_size(para.table_size),
      opt_num(para.opt_num),
      thread_num(para.thread_num),
      if_resize(para.if_resize),
      load_factor(para.load_factor),
      hit_ratio(para.hit_percent),
      rand_mem_free(para.rand_mem_free) {

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
        case BenchmarkObjectType::BINAWARECHAINEDHT:
            obj =
                new BenchmarkBinAwareChained(table_size * 1.11, para.bin_size);
            // new BenchmarkBinAwareChained(table_size * 1.06, para.bin_size);
            break;
        case BenchmarkObjectType::SAMEBINCHAINEDHT:
            obj = new BenchmarkSameBinChained(table_size * 1.06, para.bin_size);
            break;
        case BenchmarkObjectType::YARDEDTTPHT:
            obj = new BenchmarkYardedTPHT(table_size * 1.03, para.bin_size);
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
        case BenchmarkObjectType::SKULKERHT:
            obj = new BenchmarkSkulkerHT(table_size, para.bin_size);
            break;
        case BenchmarkObjectType::GROWT:
            obj = new BenchmarkGrowt(table_size);
            break;
        case BenchmarkObjectType::CONCURRENT_SKULKERHT:
            obj = new BenchmarkConcSkulkerHT(table_size, para.bin_size);
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
                } else if (para.object_id ==
                           BenchmarkObjectType::BINAWARECHAINEDHT) {
                    obj =
                        new BenchmarkBinAwareChained(table_size, para.bin_size);
                } else if (para.object_id ==
                           BenchmarkObjectType::SAMEBINCHAINEDHT) {
                    obj =
                        new BenchmarkSameBinChained(table_size, para.bin_size);
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

                if (para.object_id == BenchmarkObjectType::BYTEARRAYCHAINEDHT ||
                    para.object_id == BenchmarkObjectType::BINAWARECHAINEDHT ||
                    para.object_id == BenchmarkObjectType::SAMEBINCHAINEDHT) {

                    output_stream << "Bin Size: " << para.bin_size << std::endl;

                    output_stream << "Avg Chain Length: "
                                  << dynamic_cast<BenchmarkChained*>(obj)
                                         ->AvgChainLength()
                                  << std::endl;

                    uint32_t max_chain_length;

                    output_stream << "Max Chain Length: "
                                  << (max_chain_length =
                                          dynamic_cast<BenchmarkChained*>(obj)
                                              ->MaxChainLength())
                                  << std::endl;

                    auto hist = dynamic_cast<BenchmarkChained*>(obj)
                                    ->ChainLengthHistogram();
                    output_stream << "Chain Length Histogram: " << std::endl;
                    output_stream << "\tLength\t\tCount" << std::endl;
                    for (uint32_t i = 0; i <= max_chain_length; ++i) {
                        output_stream << "\t" << i << "\t\t" << hist[i]
                                      << std::endl;
                    }

                    if (para.object_id ==
                        BenchmarkObjectType::BINAWARECHAINEDHT) {
                        auto hist = dynamic_cast<BenchmarkBinAwareChained*>(obj)
                                        ->DoubleSlotStatistics();

                        output_stream << "Total Double Slot: " << hist[0]
                                      << std::endl;
                        output_stream
                            << "Max Double Slot Length: " << hist[1] * 2
                            << std::endl;
                        output_stream << "Double Slot Statistics: "
                                      << std::endl;
                        output_stream << "\tLength\t\tCount" << std::endl;
                        for (uint32_t i = 0; i <= hist[1]; ++i) {
                            output_stream << "\t" << i * 2 << "\t\t"
                                          << hist[i + 2] << std::endl;
                        }
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
            run = [this, para]() {
                obj_fill(table_size);

                std::vector<uint64_t> query_key_vec;
                std::vector<uint8_t> query_ptr_vec;
                batch_query_vec_prepare(query_key_vec, query_ptr_vec, opt_num,
                                        1);

                std::clock_t start = std::clock();

                batch_query(query_key_vec, query_ptr_vec, opt_num);

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

                if (para.object_id == BenchmarkObjectType::BYTEARRAYCHAINEDHT) {
                    output_stream
                        << "Query Entry Count: "
                        << dynamic_cast<BenchmarkByteArrayChained*>(obj)
                               ->QueryEntryCnt()
                        << std::endl;
                }
                if (para.object_id == BenchmarkObjectType::SKULKERHT) {
                    output_stream << "Query Entry Count: "
                                  << dynamic_cast<BenchmarkSkulkerHT*>(obj)
                                         ->QueryEntryCnt()
                                  << std::endl;
                }
            };
            break;
        case BenchmarkCaseType::QUERY_MISS_ONLY:
            run = [this]() {
                obj_fill(table_size);

                std::vector<uint64_t> query_key_vec;
                std::vector<uint8_t> query_ptr_vec;
                batch_query_vec_prepare(query_key_vec, query_ptr_vec, opt_num,
                                        0);

                std::clock_t start = std::clock();

                batch_query(query_key_vec, query_ptr_vec, opt_num);

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

                std::vector<uint64_t> query_key_vec;
                std::vector<uint8_t> query_ptr_vec;
                batch_query_vec_prepare(query_key_vec, query_ptr_vec, opt_num,
                                        hit_ratio);

                std::clock_t start = std::clock();

                batch_query(query_key_vec, query_ptr_vec, opt_num);

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

                std::vector<uint64_t> query_key_vec;
                std::vector<uint8_t> query_ptr_vec;
                batch_query_vec_prepare(query_key_vec, query_ptr_vec, opt_num,
                                        1);

                std::clock_t start = std::clock();

                batch_query(query_key_vec, query_ptr_vec, opt_num);

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

                std::vector<uint64_t> query_key_vec;
                std::vector<uint8_t> query_ptr_vec;
                batch_query_vec_prepare(query_key_vec, query_ptr_vec, opt_num,
                                        0);

                std::clock_t start = std::clock();

                batch_query(query_key_vec, query_ptr_vec, opt_num);

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

                std::vector<uint64_t> query_key_vec;
                std::vector<uint8_t> query_ptr_vec;
                batch_query_vec_prepare(query_key_vec, query_ptr_vec, opt_num,
                                        hit_ratio);

                std::clock_t start = std::clock();

                batch_query(query_key_vec, query_ptr_vec, opt_num);

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

                    std::vector<uint64_t> query_key_vec;
                    std::vector<uint8_t> query_ptr_vec;
                    batch_query_vec_prepare(query_key_vec, query_ptr_vec,
                                            opt_num, 0);

                    std::clock_t start = std::clock();

                    batch_query(query_key_vec, query_ptr_vec, opt_num);

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

                    dynamic_cast<BenchmarkByteArrayChained*>(obj)
                        ->set_chain_length(chain_length);

                    gen_rand_vector(case_table_size);

                    std::clock_t start = std::clock();

                    batch_query_no_mem(opt_num, 1);
                    // batch_query_no_mem(opt_num, 0);

                    auto end = std::clock();
                    auto duration = end - start;

                    output_stream << "Query No Mem" << std::endl;
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
        case BenchmarkCaseType::INSERT_DELETE_LOAD_FACTOR_SUPPORT:
            run = [this, para]() {
                if (para.object_id == BenchmarkObjectType::BYTEARRAYCHAINEDHT) {
                    obj = new BenchmarkByteArrayChained(
                        table_size, para.quotienting_tail_length,
                        para.bin_size);
                } else if (para.object_id ==
                           BenchmarkObjectType::BINAWARECHAINEDHT) {
                    obj =
                        new BenchmarkBinAwareChained(table_size, para.bin_size);
                } else if (para.object_id ==
                           BenchmarkObjectType::SAMEBINCHAINEDHT) {
                    obj =
                        new BenchmarkSameBinChained(table_size, para.bin_size);
                }
                std::clock_t start = std::clock();

                double op_ratio = 0.000001;

                uint64_t opt_cnt = insert_delete_opt_to_overflow(
                    table_size, para.bin_size, op_ratio, load_factor);

                auto end = std::clock();
                auto duration = end - start;

                output_stream << "Table Size: " << table_size << std::endl;
                output_stream << "Bin Size: " << para.bin_size << std::endl;
                output_stream << "Load Factor: " << load_factor << std::endl;
                output_stream << "Operation Ratio: " << op_ratio << std::endl;
                output_stream << "Operation Capacity: " << opt_cnt << std::endl;
                output_stream << "Op/Size Ratio: " << 1.0 * opt_cnt / table_size
                              << std::endl;
                output_stream
                    << "CPU Time: "
                    << int(1000.0 * (std::clock() - start) / CLOCKS_PER_SEC)
                    << " ms" << std::endl;

                output_stream
                    << "Throughput: "
                    << int(double(opt_cnt) / double(duration) * CLOCKS_PER_SEC)
                    << " ops/s" << std::endl;

                output_stream
                    << "Latency: "
                    << int(double(duration) / double(opt_cnt) / CLOCKS_PER_SEC *
                           (1ll * 1000 * 1000 * 1000))
                    << " ns/op" << std::endl;

                if (para.object_id == BenchmarkObjectType::BYTEARRAYCHAINEDHT ||
                    para.object_id == BenchmarkObjectType::SAMEBINCHAINEDHT ||
                    para.object_id == BenchmarkObjectType::BINAWARECHAINEDHT) {
                    output_stream << "Avg Chain Length: "
                                  << dynamic_cast<BenchmarkChained*>(obj)
                                         ->AvgChainLength()
                                  << std::endl;

                    uint32_t max_chain_length;

                    output_stream << "Max Chain Length: "
                                  << (max_chain_length =
                                          dynamic_cast<BenchmarkChained*>(obj)
                                              ->MaxChainLength())
                                  << std::endl;

                    auto hist = dynamic_cast<BenchmarkChained*>(obj)
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
        case BenchmarkCaseType::YCSB_A:
        case BenchmarkCaseType::YCSB_B:
        case BenchmarkCaseType::YCSB_C:
            run = [this, para]() {
                std::vector<uint64_t> ycsb_keys;
                ycsb_load(ycsb_keys, para.ycsb_load_path);
                std::vector<std::pair<uint64_t, uint64_t>> ycsb_exe_vec;
                ycsb_exe_load(ycsb_exe_vec, para.ycsb_exe_path);

                std::clock_t start = std::clock();

                obj->YCSBFill(ycsb_keys, para.thread_num);

                auto start_fill = std::clock();
                auto fill_duration = start_fill - start;

                obj->YCSBRun(ycsb_exe_vec, para.thread_num);

                auto start_run = std::clock();
                auto run_duration = start_run - start_fill;

                auto end = std::clock();
                auto duration = end - start;

                int fill_op_cnt = ycsb_keys.size();
                int run_op_cnt = ycsb_exe_vec.size();

                output_stream
                    << "CPU Time: " << int(1000.0 * (duration) / CLOCKS_PER_SEC)
                    << " ms" << std::endl;
                output_stream << "Fill Time: "
                              << int(1000.0 * (fill_duration) / CLOCKS_PER_SEC)
                              << " ms" << std::endl;
                output_stream << "Run Time: "
                              << int(1000.0 * (run_duration) / CLOCKS_PER_SEC)
                              << " ms" << std::endl;
                output_stream
                    << "Fill Latency: "
                    << int(double(fill_duration) / double(fill_op_cnt) /
                           CLOCKS_PER_SEC * (1ll * 1000 * 1000 * 1000))
                    << " ns/op" << std::endl;
                output_stream
                    << "Run Latency: "
                    << int(double(run_duration) / double(run_op_cnt) /
                           CLOCKS_PER_SEC * (1ll * 1000 * 1000 * 1000))
                    << " ns/op" << std::endl;
                output_stream << "Fill Throughput: "
                              << int(double(fill_op_cnt) /
                                     double(fill_duration) * CLOCKS_PER_SEC)
                              << " ops/s" << std::endl;
                output_stream << "Run Throughput: "
                              << int(double(run_op_cnt) / double(run_duration) *
                                     CLOCKS_PER_SEC)
                              << " ops/s" << std::endl;
            };
            break;

        default:
            abort();
    }
}

void Benchmark::Run() {
    this->run();

    if (rand_mem_free) {
        sleep(1);
    }
}
}  // namespace tinyptr
