#include "benchmark.h"
#include <unistd.h>
#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstdlib>
#include <ctime>
#include <fstream>
#include <iostream>
#include <iterator>
#include <ostream>
#include <queue>
#include <random>
#include <set>
#include <sstream>
#include <unordered_set>
#include "../chained_ht_64.h"
#include "../dereference_table_64.h"
#include "benchmark/benchmark_nonconc_blast_ht.h"
#include "benchmark_bin_aware_chainedht.h"
#include "benchmark_blast_ht.h"
#include "benchmark_bolt_ht.h"
#include "benchmark_bytearray_chainedht.h"
#include "benchmark_chained.h"
#include "benchmark_chainedht64.h"
#include "benchmark_clht.h"
#include "benchmark_cli_para.h"
#include "benchmark_conc_bytearray_ht.h"
#include "benchmark_conc_skulkerht.h"
#include "benchmark_cuckoo.h"
#include "benchmark_dereftab64.h"
#include "benchmark_hash_distribution.h"
// #include "benchmark_growt.h"
#include <iomanip>
#include "benchmark_iceberg.h"
#include "benchmark_intarray64.h"
#include "benchmark_junction.h"
#include "benchmark_object_64.h"
#include "benchmark_resizable_blastht.h"
#include "benchmark_resizable_bytearray_ht.h"
#include "benchmark_resizable_skulkerht.h"
#include "benchmark_same_bin_chainedht.h"
#include "benchmark_skulkerht.h"
#include "benchmark_stagger_bytearray_ht.h"
#include "benchmark_std_unordered_map_64.h"
#include "benchmark_tbb.h"
#include "benchmark_yarded_tp_ht.h"

namespace tinyptr {

uint8_t Benchmark::gen_rand_nonzero_8() {
    uint8_t msk = ~(uint8_t(0));
    uint64_t res = rgen64();
    while (0 == (res & msk))
        res = rgen64();
    return (res & msk);
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

// uint64_t Benchmark::rgen64() {
//     return rgen64();
// }

uint64_t Benchmark::gen_value() {
    return rgen64();
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
                dynamic_cast_obj->Erase(key, rgen64());
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

void Benchmark::obj_fill_vec_prepare(std::vector<uint64_t>& key_vec,
                                     std::vector<uint64_t>& value_vec,
                                     int ins_cnt) {
    key_vec.clear();
    value_vec.clear();
    for (int i = 0; i < ins_cnt; ++i) {
        key_vec.push_back(gen_key_hittable());
        value_vec.push_back(gen_value());
    }
}

void Benchmark::obj_fill(std::vector<uint64_t>& key_vec,
                         std::vector<uint64_t>& value_vec) {
    for (int i = 0; i < key_vec.size(); ++i) {
        obj->Insert(key_vec[i], value_vec[i]);
    }
}

void Benchmark::obj_fill_mem_free(int ins_cnt) {
    for (int i = 0; i < ins_cnt; ++i) {
        obj->Insert(gen_key_hittable(), gen_value());
    }
}

void Benchmark::batch_query_vec_prepare(std::vector<uint64_t>& key_vec,
                                        std::vector<uint64_t>& query_key_vec,
                                        uint64_t op_cnt, double hit_rate) {
    query_key_vec.clear();
    // TODO: consider ptr here

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

    if (zipfian_skew > kEps) {
        ZipfianGenerator zipfian_generator(key_ind_range, zipfian_skew);
        while (op_cnt--) {
            uint64_t key;

            if (hit_bar >= (gen_key_hittable() & (~(uint32_t(0))))) {
                int key_ind = zipfian_generator.next();
                key = key_vec[key_ind];
                // TODO: consider ptr here
            } else {
                key = gen_key_miss();
                // TODO: consider ptr here
            }

            query_key_vec.push_back(key);
        }
    } else {

        while (op_cnt--) {
            uint64_t key;

            if (hit_bar >= (gen_key_hittable() & (~(uint32_t(0))))) {
                int key_ind = rgen64() % key_ind_range;
                key = key_vec[key_ind];
                // TODO: consider ptr here
            } else {
                key = gen_key_miss();
                // TODO: consider ptr here
            }

            query_key_vec.push_back(key);
        }
    }
}

void Benchmark::batch_query(std::vector<uint64_t>& query_key_vec) {
    for (uint64_t i = 0; i < query_key_vec.size(); ++i) {
        obj->Query(query_key_vec[i], 0);
    }
}

void Benchmark::batch_query_no_mem(std::vector<uint64_t>& key_vec,
                                   int query_cnt, double hit_rate) {
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

        if (hit_bar >= (gen_key_hittable() & (~(uint32_t(0))))) {
            int key_ind = rgen64() % key_ind_range;
            key = key_vec[key_ind];
            ptr = gen_rand_nonzero_8();
        } else {
            key = gen_key_miss();
            ptr = gen_rand_nonzero_8();
        }

        uint64_t value;
        dynamic_cast<BenchmarkByteArrayChained*>(obj)->QueryNoMem(key, &value);
    }
}

void Benchmark::batch_update_vec_prepare(
    std::vector<uint64_t>& key_vec, std::vector<uint64_t>& update_key_vec,
    std::vector<uint64_t>& update_value_vec, int update_cnt) {
    update_key_vec.clear();
    update_value_vec.clear();
    for (int i = 0; i < update_cnt; ++i) {
        update_key_vec.push_back(key_vec[rgen64() % key_vec.size()]);
        update_value_vec.push_back(gen_value());
    }
}

void Benchmark::batch_update(std::vector<uint64_t>& update_key_vec,
                             std::vector<uint64_t>& update_value_vec) {
    for (uint64_t i = 0; i < update_key_vec.size(); ++i) {
        obj->Update(update_key_vec[i], 0, update_value_vec[i]);
    }
}

void Benchmark::erase_vec_prepare(std::vector<uint64_t>& key_vec,
                                  std::vector<uint64_t>& erase_key_vec) {
    erase_key_vec.clear();
    for (int i = 0; i < key_vec.size(); ++i)
        erase_key_vec.push_back(key_vec[i]);
    std::random_shuffle(erase_key_vec.begin(), erase_key_vec.end());
}

void Benchmark::erase_all(std::vector<uint64_t>& erase_key_vec) {
    // we assume the all the elements in the vector are inserted
    // and nothing happens after that
    // TODO: consider ptr here
    for (auto iter : erase_key_vec) {
        obj->Erase(iter, 0);
    }
}

void Benchmark::alternating_insert_erase(int opt_cnt) {
    while (opt_cnt--) {
        uint64_t key = gen_key_hittable();
        obj->Erase(key, obj->Insert(key, gen_value()));
    }
}

void Benchmark::all_operation_rand(std::vector<uint64_t>& key_vec,
                                   int opt_cnt) {
    while (opt_cnt--) {
        uint64_t key = rgen64();
        uint8_t ptr = gen_rand_nonzero_8();

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

void Benchmark::ycsb_load(std::vector<uint64_t>& ycsb_keys, std::string path,
                          double load_factor_) {
    // Use a larger buffer size for better performance
    const size_t BUFFER_SIZE = 1 << 20;  // 1MB buffer

    FILE* file = fopen(path.c_str(), "r");
    if (!file)
        return;

    fseek(file, 0, SEEK_END);
    long fileSize = ftell(file);
    fseek(file, 0, SEEK_SET);

    char* buffer = new char[BUFFER_SIZE];

    std::string partialLine;

    while (!feof(file)) {
        size_t bytesRead = fread(buffer, 1, BUFFER_SIZE - 1, file);
        if (bytesRead == 0)
            break;

        buffer[bytesRead] = '\0';

        // Process each line in the buffer
        char* pos = buffer;
        char* end = buffer + bytesRead;

        while (pos < end) {
            char* lineEnd = strchr(pos, '\n');

            if (!lineEnd) {
                if (feof(file)) {
                    uint64_t key = strtoull(pos + 7, nullptr, 10);
                    ycsb_keys.push_back(key);
                } else {
                    partialLine += pos;
                }
                break;
            }

            *lineEnd = '\0';  // Terminate the line

            if (!partialLine.empty()) {
                partialLine += pos;
                uint64_t key = strtoull(partialLine.c_str() + 7, nullptr, 10);
                ycsb_keys.push_back(key);
                partialLine.clear();
            } else {
                uint64_t key = strtoull(pos + 7, nullptr, 10);
                ycsb_keys.push_back(key);
            }

            pos = lineEnd + 1;
        }
    }

    // Process any remaining partial line from the last buffer
    if (!partialLine.empty() && partialLine.length() > 7) {
        uint64_t key = strtoull(partialLine.c_str() + 7, nullptr, 10);
        ycsb_keys.push_back(key);
    }

    delete[] buffer;
    fclose(file);

    // Apply load factor to keep only a portion of the keys
    if (load_factor_ < 1.0 && ycsb_keys.size() > 0) {
        size_t keep_count =
            static_cast<size_t>(ycsb_keys.size() * load_factor_);
        ycsb_keys.resize(keep_count);
    }
}

void Benchmark::ycsb_exe_load(
    std::vector<std::pair<uint64_t, uint64_t>>& ycsb_exe_vec, std::string path,
    double load_factor_, BlastHT* available_keys) {

    // Use a larger buffer size for better performance
    const size_t BUFFER_SIZE = 1 << 20;  // 1MB buffer

    FILE* file = fopen(path.c_str(), "r");
    if (!file)
        return;

    fseek(file, 0, SEEK_END);
    long fileSize = ftell(file);
    fseek(file, 0, SEEK_SET);

    char* buffer = new char[BUFFER_SIZE];

    std::string partialLine;

    while (!feof(file)) {
        size_t bytesRead = fread(buffer, 1, BUFFER_SIZE - 1, file);
        if (bytesRead == 0)
            break;

        buffer[bytesRead] = '\0';

        // Process each line in the buffer
        char* pos = buffer;
        char* end = buffer + bytesRead;

        while (pos < end) {
            char* lineEnd = strchr(pos, '\n');

            if (!lineEnd) {
                if (feof(file)) {
                    uint64_t key;
                    if (strncmp(pos, "INSERT", 6) == 0) {
                        key = strtoull(pos + 7, nullptr, 10);
                        ycsb_exe_vec.emplace_back(1, key);
                    } else if (strncmp(pos, "READ", 4) == 0) {
                        key = strtoull(pos + 5, nullptr, 10);
                        ycsb_exe_vec.emplace_back(0, key);
                    }
                } else {
                    partialLine += pos;
                }
                break;
            }

            *lineEnd = '\0';  // Terminate the line

            if (!partialLine.empty()) {
                partialLine += pos;
                uint64_t key;
                if (strncmp(partialLine.c_str(), "INSERT", 6) == 0) {
                    key = strtoull(partialLine.c_str() + 7, nullptr, 10);
                    ycsb_exe_vec.emplace_back(1, key);
                } else if (strncmp(partialLine.c_str(), "READ", 4) == 0) {
                    key = strtoull(partialLine.c_str() + 5, nullptr, 10);
                    ycsb_exe_vec.emplace_back(0, key);
                }
                partialLine.clear();
            } else {
                uint64_t key;
                if (strncmp(pos, "INSERT", 6) == 0) {
                    key = strtoull(pos + 7, nullptr, 10);
                    ycsb_exe_vec.emplace_back(1, key);
                } else if (strncmp(pos, "READ", 4) == 0) {
                    key = strtoull(pos + 5, nullptr, 10);
                    ycsb_exe_vec.emplace_back(0, key);
                }
            }

            pos = lineEnd + 1;
        }
    }

    // Process any remaining partial line from the last buffer
    if (!partialLine.empty()) {
        uint64_t key;
        if (strncmp(partialLine.c_str(), "INSERT", 6) == 0) {
            key = strtoull(partialLine.c_str() + 7, nullptr, 10);
            ycsb_exe_vec.emplace_back(1, key);
        } else if (strncmp(partialLine.c_str(), "READ", 4) == 0) {
            key = strtoull(partialLine.c_str() + 5, nullptr, 10);
            ycsb_exe_vec.emplace_back(0, key);
        }
    }

    delete[] buffer;
    fclose(file);

    // Apply load factor proportionally while maintaining EXACT order and ensuring key existence
    if (load_factor_ < 1.0 && ycsb_exe_vec.size() > 0) {
        // Count original operation types
        size_t original_inserts = 0;
        size_t original_reads = 0;
        for (const auto& op : ycsb_exe_vec) {
            if (op.first == 1)
                original_inserts++;
            else
                original_reads++;
        }

        // Calculate target counts to maintain ratio
        size_t target_inserts =
            static_cast<size_t>(std::round(original_inserts * load_factor_));
        size_t target_reads =
            static_cast<size_t>(std::round(original_reads * load_factor_));
        size_t target_total = target_inserts + target_reads;

        // Build available keys set (loaded keys + run phase inserts we'll keep)

        // Two-pass algorithm to maintain exact order and validate keys

        // Pass 1: Mark inserts to keep and build available keys
        std::vector<bool> keep_op(ycsb_exe_vec.size(), false);
        size_t inserts_kept = 0;

        for (size_t i = 0;
             i < ycsb_exe_vec.size() && inserts_kept < target_inserts; ++i) {
            if (ycsb_exe_vec[i].first == 1) {  // INSERT
                keep_op[i] = true;
                available_keys->Insert(ycsb_exe_vec[i].second, 0);
                inserts_kept++;
            }
        }

        // Pass 2: Mark reads to keep (only valid ones that reference available keys)
        size_t reads_kept = 0;
        std::vector<size_t> invalid_read_indices;

        for (size_t i = 0; i < ycsb_exe_vec.size() && reads_kept < target_reads;
             ++i) {
            if (ycsb_exe_vec[i].first == 0) {  // READ
                uint64_t value;
                if (available_keys->Query(ycsb_exe_vec[i].second, &value)) {
                    keep_op[i] = true;
                    reads_kept++;
                } else {
                    invalid_read_indices.push_back(i);
                }
            }
        }

        // Pass 3: If we didn't reach target_reads, try to add more valid reads
        size_t current_total = inserts_kept + reads_kept;

        if (current_total < target_total) {
            for (size_t i = 0;
                 i < ycsb_exe_vec.size() && current_total < target_total; ++i) {
                if (!keep_op[i] && ycsb_exe_vec[i].first == 0) {
                    keep_op[i] = true;
                    current_total++;
                }
            }
        }

        // Build final vector maintaining EXACT original order
        std::vector<std::pair<uint64_t, uint64_t>> kept_ops;
        for (size_t i = 0; i < ycsb_exe_vec.size(); ++i) {
            if (keep_op[i]) {
                kept_ops.push_back(ycsb_exe_vec[i]);
            }
        }

        ycsb_exe_vec = std::move(kept_ops);
    }
}

void Benchmark::vec_to_ops(
    std::vector<uint64_t>& key_vec,
    std::vector<std::tuple<uint64_t, uint64_t, uint64_t>>& ops,
    uint64_t opt_type) {
    ops.clear();
    for (uint64_t i = 0; i < key_vec.size(); ++i) {
        ops.push_back(std::make_tuple(opt_type, key_vec[i], 0));
    }
}

void Benchmark::vec_to_ops(
    std::vector<uint64_t>& key_vec, std::vector<uint64_t>& value_vec,
    std::vector<std::tuple<uint64_t, uint64_t, uint64_t>>& ops,
    uint64_t opt_type) {
    ops.clear();
    for (uint64_t i = 0; i < key_vec.size(); ++i) {
        ops.push_back(std::make_tuple(opt_type, key_vec[i], value_vec[i]));
    }
}

Benchmark::Benchmark(BenchmarkCLIPara& para)
    : output_stream(para.GetOuputFileName()),
      table_size(para.table_size),
      opt_num(para.opt_num),
      thread_num(para.thread_num),
      load_factor(para.load_factor),
      hit_ratio(para.hit_percent),
      zipfian_skew(para.zipfian_skew),
      rand_mem_free(para.rand_mem_free),
      rgen64(rng::random_device_seed{}()),
      rgen128(rng::random_device_seed{}()) {

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
        // case BenchmarkObjectType::GROWT:
        //     obj = new BenchmarkGrowt(table_size);
        //     break;
        case BenchmarkObjectType::CONCURRENT_SKULKERHT:
            obj = new BenchmarkConcSkulkerHT(table_size, para.bin_size);
            break;
        case BenchmarkObjectType::CONCURRENT_BYTEARRAYCHAINEDHT:
            obj =
                new BenchmarkConcByteArrayChainedHT(table_size, para.bin_size);
            break;
        case BenchmarkObjectType::JUNCTION:
            obj = new BenchmarkJunction(table_size);
            break;
        case BenchmarkObjectType::RESIZABLE_SKULKERHT: {
            uint64_t part_num = 10;
            obj = new BenchmarkResizableSkulkerHT(table_size / part_num,
                                                  part_num, thread_num);
        } break;
        case BenchmarkObjectType::RESIZABLE_BYTEARRAYCHAINEDHT: {
            uint64_t part_num = 16;
            obj = new BenchmarkResizableByteArrayChainedHT(
                table_size / part_num, part_num, thread_num);
        } break;
        case BenchmarkObjectType::BOLT:
            obj = new BenchmarkBoltHT(table_size, para.bin_size);
            break;
        case BenchmarkObjectType::BLAST:
            obj = new BenchmarkBlastHT(table_size, para.bin_size);
            break;
        case BenchmarkObjectType::RESIZABLE_BLAST: {
            uint64_t part_num = 16;
            obj = new BenchmarkResizableBlastHT(table_size / part_num, part_num,
                                                thread_num);
        } break;
        case BenchmarkObjectType::STAGGER_BYTEARRAYCHAINEDHT: {
            uint64_t part_num = 16;
            obj = new BenchmarkStaggerByteArrayHT(table_size / part_num,
                                                  part_num, thread_num);
        } break;
        case BenchmarkObjectType::HASH_DISTRIBUTION:
            obj = new BenchmarkHashDistribution(table_size);
            break;
        case BenchmarkObjectType::NONCONC_BLAST:
            obj = new BenchmarkNonConcBlastHT(table_size, para.bin_size);
            break;
        case BenchmarkObjectType::TBB:
            obj = new BenchmarkTBB(table_size);
            break;
        default:
            abort();
    }

    switch (para.case_id) {
        case BenchmarkCaseType::INSERT_ONLY_LOAD_FACTOR_SUPPORT:
            run = [this, para]() {
                std::vector<uint64_t> key_vec, value_vec;
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
                auto start = std::chrono::high_resolution_clock::now();

                int load_cnt = insert_cnt_to_overflow();

                auto end = std::chrono::high_resolution_clock::now();
                auto duration =
                    std::chrono::duration_cast<std::chrono::milliseconds>(end -
                                                                          start)
                        .count();

                output_stream << "Table Size: " << table_size << std::endl;
                output_stream << "Load Capacity: " << load_cnt << std::endl;
                output_stream << "CPU Time: " << duration << " ms" << std::endl;

                output_stream << "Load Factor: "
                              << double(load_cnt) / double(table_size) * 100
                              << " %" << std::endl;

                output_stream << "Throughput: "
                              << int(double(load_cnt) / (duration / 1000.0))
                              << " ops/s" << std::endl;

                output_stream << "Latency: "
                              << int(duration * 1000000.0 / double(load_cnt))
                              << " ns/op" << std::endl;

                if (para.object_id == BenchmarkObjectType::BOLT) {
                    dynamic_cast<BenchmarkBoltHT*>(obj)->Stats();
                }

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
                std::vector<uint64_t> key_vec, value_vec;
                if (!rand_mem_free) {
                    obj_fill_vec_prepare(key_vec, value_vec, opt_num);
                }

                std::vector<std::tuple<uint64_t, uint64_t, uint64_t>> ops;

                if (thread_num) {
                    vec_to_ops(key_vec, value_vec, ops, ConcOptType::INSERT);
                }

                auto start = std::chrono::high_resolution_clock::now();

                if (thread_num) {
                    obj->ConcurrentRun(ops, thread_num);
                } else {
                    if (!rand_mem_free) {
                        obj_fill(key_vec, value_vec);
                    } else {
                        obj_fill_mem_free(opt_num);
                    }
                }

                auto end = std::chrono::high_resolution_clock::now();
                auto duration =
                    std::chrono::duration_cast<std::chrono::milliseconds>(end -
                                                                          start)
                        .count();

                output_stream << "CPU Time: " << duration << " ms" << std::endl;

                output_stream << "Throughput: "
                              << int(double(opt_num) / (duration / 1000.0))
                              << " ops/s" << std::endl;

                output_stream << "Latency: "
                              << int(duration * 1000000.0 / double(opt_num))
                              << " ns/op" << std::endl;
            };
            break;
        case BenchmarkCaseType::UPDATE_ONLY:
            run = [this]() {
                std::vector<uint64_t> key_vec, value_vec;
                obj_fill_vec_prepare(key_vec, value_vec, opt_num);

                std::vector<std::tuple<uint64_t, uint64_t, uint64_t>> ops;
                if (thread_num) {
                    vec_to_ops(key_vec, value_vec, ops, ConcOptType::INSERT);
                }

                if (thread_num) {
                    obj->ConcurrentRun(ops, thread_num);
                } else {
                    obj_fill(key_vec, value_vec);
                }

                std::vector<uint64_t> update_key_vec;
                std::vector<uint64_t> update_value_vec;
                batch_update_vec_prepare(key_vec, update_key_vec,
                                         update_value_vec, opt_num);

                if (thread_num) {
                    vec_to_ops(update_key_vec, update_value_vec, ops,
                               ConcOptType::UPDATE);
                }

                auto start = std::chrono::high_resolution_clock::now();

                if (thread_num) {
                    obj->ConcurrentRun(ops, thread_num);
                } else {
                    batch_update(update_key_vec, update_value_vec);
                }

                auto end = std::chrono::high_resolution_clock::now();
                auto duration =
                    std::chrono::duration_cast<std::chrono::milliseconds>(end -
                                                                          start)
                        .count();

                output_stream << "CPU Time: " << duration << " ms" << std::endl;

                output_stream << "Throughput: "
                              << int(double(opt_num) / (duration / 1000.0))
                              << " ops/s" << std::endl;

                output_stream << "Latency: "
                              << int(duration * 1000000.0 / double(opt_num))
                              << " ns/op" << std::endl;
            };
            break;
        case BenchmarkCaseType::ERASE_ONLY:
            run = [this]() {
                std::vector<uint64_t> key_vec, value_vec;
                obj_fill_vec_prepare(key_vec, value_vec, opt_num);

                std::vector<std::tuple<uint64_t, uint64_t, uint64_t>> ops;
                if (thread_num) {
                    vec_to_ops(key_vec, value_vec, ops, ConcOptType::INSERT);
                }

                if (thread_num) {
                    obj->ConcurrentRun(ops, thread_num);
                } else {
                    obj_fill(key_vec, value_vec);
                }

                std::vector<uint64_t> erase_key_vec;
                erase_vec_prepare(key_vec, erase_key_vec);

                if (thread_num) {
                    vec_to_ops(erase_key_vec, ops, ConcOptType::ERASE);
                }

                auto start = std::chrono::high_resolution_clock::now();

                if (thread_num) {
                    obj->ConcurrentRun(ops, thread_num);
                } else {
                    erase_all(erase_key_vec);
                }

                auto end = std::chrono::high_resolution_clock::now();
                auto duration =
                    std::chrono::duration_cast<std::chrono::milliseconds>(end -
                                                                          start)
                        .count();

                output_stream << "CPU Time: " << duration << " ms" << std::endl;

                output_stream << "Throughput: "
                              << int(double(opt_num) / (duration / 1000.0))
                              << " ops/s" << std::endl;

                output_stream << "Latency: "
                              << int(duration * 1000000.0 / double(opt_num))
                              << " ns/op" << std::endl;
            };
            break;
        case BenchmarkCaseType::ALTERNATING_INSERT_ERASE:
            run = [this]() {
                auto start = std::chrono::high_resolution_clock::now();

                alternating_insert_erase(opt_num);

                auto end = std::chrono::high_resolution_clock::now();
                auto duration =
                    std::chrono::duration_cast<std::chrono::milliseconds>(end -
                                                                          start)
                        .count();

                output_stream << "CPU Time: " << duration << " ms" << std::endl;

                output_stream << "Throughput: "
                              << int(double(opt_num) / (duration / 1000.0))
                              << " ops/s" << std::endl;

                output_stream << "Latency: "
                              << int(duration * 1000000.0 / double(opt_num))
                              << " ns/op" << std::endl;
            };
            break;
        case BenchmarkCaseType::QUERY_HIT_ONLY:
            run = [this, para]() {
                std::vector<uint64_t> key_vec, value_vec;
                obj_fill_vec_prepare(key_vec, value_vec, opt_num);

                std::vector<std::tuple<uint64_t, uint64_t, uint64_t>> ops;
                if (thread_num) {
                    vec_to_ops(key_vec, value_vec, ops, ConcOptType::INSERT);
                }

                if (thread_num) {
                    obj->ConcurrentRun(ops, thread_num);
                } else {
                    obj_fill(key_vec, value_vec);
                }

                std::vector<uint64_t> query_key_vec;
                std::vector<uint8_t> query_ptr_vec;
                batch_query_vec_prepare(key_vec, query_key_vec, opt_num, 1);

                if (thread_num) {
                    vec_to_ops(query_key_vec, ops, ConcOptType::QUERY);
                }

                auto start = std::chrono::high_resolution_clock::now();

                if (thread_num) {
                    obj->ConcurrentRun(ops, thread_num);
                } else {
                    batch_query(query_key_vec);
                }

                auto end = std::chrono::high_resolution_clock::now();
                auto duration =
                    std::chrono::duration_cast<std::chrono::milliseconds>(end -
                                                                          start)
                        .count();

                output_stream << "CPU Time: " << duration << " ms" << std::endl;

                output_stream << "Throughput: "
                              << int(double(opt_num) / (duration / 1000.0))
                              << " ops/s" << std::endl;

                output_stream << "Latency: "
                              << int(duration * 1000000.0 / double(opt_num))
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
            hit_ratio = 0;
        case BenchmarkCaseType::QUERY_HIT_PERCENT:
            run = [this]() {
                std::vector<uint64_t> key_vec, value_vec;
                obj_fill_vec_prepare(key_vec, value_vec, opt_num);

                std::vector<std::tuple<uint64_t, uint64_t, uint64_t>> ops;
                if (thread_num) {
                    vec_to_ops(key_vec, value_vec, ops, ConcOptType::INSERT);
                }

                if (thread_num) {
                    obj->ConcurrentRun(ops, thread_num);
                } else {
                    obj_fill(key_vec, value_vec);
                }

                std::vector<uint64_t> query_key_vec;
                std::vector<uint8_t> query_ptr_vec;
                batch_query_vec_prepare(key_vec, query_key_vec, opt_num,
                                        hit_ratio);

                if (thread_num) {
                    vec_to_ops(query_key_vec, ops, ConcOptType::QUERY);
                }

                auto start = std::chrono::high_resolution_clock::now();

                if (thread_num) {
                    obj->ConcurrentRun(ops, thread_num);
                } else {
                    batch_query(query_key_vec);
                }

                auto end = std::chrono::high_resolution_clock::now();
                auto duration =
                    std::chrono::duration_cast<std::chrono::milliseconds>(end -
                                                                          start)
                        .count();

                output_stream << "CPU Time: " << duration << " ms" << std::endl;

                output_stream << "Throughput: "
                              << int(double(opt_num) / (duration / 1000.0))
                              << " ops/s" << std::endl;

                output_stream << "Latency: "
                              << int(duration * 1000000.0 / double(opt_num))
                              << " ns/op" << std::endl;
            };
            break;
        case BenchmarkCaseType::QUERY_HIT_ONLY_CUSTOM_LOAD_FACTOR:
            hit_ratio = 1;
            goto query_load_factor;
        case BenchmarkCaseType::QUERY_MISS_ONLY_CUSTOM_LOAD_FACTOR:
            hit_ratio = 0;
        case BenchmarkCaseType::QUERY_HIT_PERCENT_CUSTOM_LOAD_FACTOR:
        query_load_factor:
            run = [this]() {
                std::vector<uint64_t> key_vec, value_vec;
                obj_fill_vec_prepare(key_vec, value_vec,
                                     int(floor(table_size * load_factor)));

                std::vector<std::tuple<uint64_t, uint64_t, uint64_t>> ops;
                if (thread_num) {
                    vec_to_ops(key_vec, value_vec, ops, ConcOptType::INSERT);
                }

                auto start = std::chrono::high_resolution_clock::now();

                if (thread_num) {
                    obj->ConcurrentRun(ops, thread_num);
                } else {
                    obj_fill(key_vec, value_vec);
                }

                auto end = std::chrono::high_resolution_clock::now();
                auto duration =
                    std::chrono::duration_cast<std::chrono::milliseconds>(end -
                                                                          start)
                        .count();

                output_stream << "Insert CPU Time: " << duration << " ms"
                              << std::endl;

                output_stream << "Insert Throughput: "
                              << int(double(table_size * load_factor) /
                                     (duration / 1000.0))
                              << " ops/s" << std::endl;

                output_stream << "Insert Latency: "
                              << int(duration * 1000000.0 /
                                     double(table_size * load_factor))
                              << " ns/op" << std::endl;

                std::vector<uint64_t> query_key_vec;
                std::vector<uint8_t> query_ptr_vec;
                batch_query_vec_prepare(key_vec, query_key_vec, opt_num,
                                        hit_ratio);

                if (thread_num) {
                    vec_to_ops(query_key_vec, ops, ConcOptType::QUERY);
                }

                start = std::chrono::high_resolution_clock::now();

                if (thread_num) {
                    obj->ConcurrentRun(ops, thread_num);
                } else {
                    batch_query(query_key_vec);
                }

                end = std::chrono::high_resolution_clock::now();
                duration =
                    std::chrono::duration_cast<std::chrono::milliseconds>(end -
                                                                          start)
                        .count();

                output_stream << "Query CPU Time: " << duration << " ms"
                              << std::endl;

                output_stream << "Query Throughput: "
                              << int(double(opt_num) / (duration / 1000.0))
                              << " ops/s" << std::endl;

                output_stream << "Query Latency: "
                              << int(duration * 1000000.0 / double(opt_num))
                              << " ns/op" << std::endl;
            };
            break;
        case BenchmarkCaseType::ALL_OPERATION_RAND:
            run = [this]() {
                std::vector<uint64_t> key_vec, value_vec;
                auto start = std::chrono::high_resolution_clock::now();

                all_operation_rand(key_vec, opt_num);

                auto end = std::chrono::high_resolution_clock::now();
                auto duration =
                    std::chrono::duration_cast<std::chrono::milliseconds>(end -
                                                                          start)
                        .count();

                output_stream << "CPU Time: " << duration << " ms" << std::endl;

                output_stream << "Throughput: "
                              << int(double(opt_num) / (duration / 1000.0))
                              << " ops/s" << std::endl;

                output_stream << "Latency: "
                              << int(duration * 1000000.0 / double(opt_num))
                              << " ns/op" << std::endl;
            };
            break;
        case BenchmarkCaseType::XXHASH64_THROUGHPUT:
            obj = new BenchmarkIntArray64(1);
            run = [this]() {
                auto start = std::chrono::high_resolution_clock::now();

                auto tmp = rgen64();
                for (int i = 0; i < opt_num; ++i) {
                    // SlowXXHash64::hash(&tmp, sizeof(uint64_t), 233);
                    XXH64(&tmp, sizeof(uint64_t), 233);
                }

                auto end = std::chrono::high_resolution_clock::now();
                auto duration =
                    std::chrono::duration_cast<std::chrono::milliseconds>(end -
                                                                          start)
                        .count();

                output_stream << "CPU Time: " << duration << " ms" << std::endl;

                output_stream << "Throughput: "
                              << int(double(opt_num) / (duration / 1000.0))
                              << " ops/s" << std::endl;

                output_stream << "Latency: "
                              << int(duration * 1000000.0 / double(opt_num))
                              << " ns/op" << std::endl;
            };
            break;
        case BenchmarkCaseType::PRNG_THROUGHPUT:
            obj = new BenchmarkIntArray64(1);
            run = [this]() {
                auto start = std::chrono::high_resolution_clock::now();

                auto tmp = rgen64();
                for (int i = 0; i < opt_num; ++i) {
                    tmp = rgen64();
                }

                auto end = std::chrono::high_resolution_clock::now();
                auto duration =
                    std::chrono::duration_cast<std::chrono::milliseconds>(end -
                                                                          start)
                        .count();

                output_stream << "CPU Time: " << duration << " ms" << std::endl;

                output_stream << "Throughput: "
                              << int(double(opt_num) / (duration / 1000.0))
                              << " ops/s" << std::endl;

                output_stream << "Latency: "
                              << int(duration * 1000000.0 / double(opt_num))
                              << " ns/op" << std::endl;
            };
            break;
        case BenchmarkCaseType::LATENCY_VARYING_CHAINLENGTH:
            run = [this, para]() {
                std::vector<uint64_t> key_vec, value_vec;

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

                    std::vector<uint64_t> query_key_vec;

                    batch_query_vec_prepare(key_vec, query_key_vec, opt_num, 0);

                    auto start = std::chrono::high_resolution_clock::now();

                    batch_query(query_key_vec);

                    auto end = std::chrono::high_resolution_clock::now();
                    auto duration =
                        std::chrono::duration_cast<std::chrono::milliseconds>(
                            end - start)
                            .count();

                    output_stream << "Table Size: " << case_table_size
                                  << std::endl;
                    output_stream << "Chain Length: " << int(chain_length)
                                  << std::endl;

                    output_stream << "CPU Time: " << duration << " ms"
                                  << std::endl;

                    output_stream << "Throughput: "
                                  << int(double(opt_num) / (duration / 1000.0))
                                  << " ops/s" << std::endl;

                    output_stream << "Latency: "
                                  << int(duration * 1000000.0 / double(opt_num))
                                  << " ns/op" << std::endl;

                    output_stream << std::endl;
                }
            };
            break;
        case BenchmarkCaseType::QUERY_NO_MEM:
            run = [this, para]() {
                std::vector<uint64_t> key_vec, value_vec;
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

                    auto start = std::chrono::high_resolution_clock::now();

                    batch_query_no_mem(key_vec, opt_num, 1);
                    // batch_query_no_mem(opt_num, 0);

                    auto end = std::chrono::high_resolution_clock::now();
                    auto duration =
                        std::chrono::duration_cast<std::chrono::milliseconds>(
                            end - start)
                            .count();

                    output_stream << "Query No Mem" << std::endl;
                    output_stream << "Table Size: " << case_table_size
                                  << std::endl;
                    output_stream << "Chain Length: " << int(chain_length)
                                  << std::endl;

                    output_stream << "CPU Time: " << duration << " ms"
                                  << std::endl;

                    output_stream << "Throughput: "
                                  << int(double(opt_num) / (duration / 1000.0))
                                  << " ops/s" << std::endl;

                    output_stream << "Latency: "
                                  << int(duration * 1000000.0 / double(opt_num))
                                  << " ns/op" << std::endl;

                    output_stream << std::endl;
                }
            };
            break;
        case BenchmarkCaseType::INSERT_DELETE_LOAD_FACTOR_SUPPORT:
            run = [this, para]() {
                std::vector<uint64_t> key_vec, value_vec;
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
                auto start = std::chrono::high_resolution_clock::now();

                double op_ratio = 0.000001;

                uint64_t opt_cnt = insert_delete_opt_to_overflow(
                    table_size, para.bin_size, op_ratio, load_factor);

                auto end = std::chrono::high_resolution_clock::now();
                auto duration =
                    std::chrono::duration_cast<std::chrono::milliseconds>(end -
                                                                          start)
                        .count();

                output_stream << "Table Size: " << table_size << std::endl;
                output_stream << "Bin Size: " << para.bin_size << std::endl;
                output_stream << "Load Factor: " << load_factor << std::endl;
                output_stream << "Operation Ratio: " << op_ratio << std::endl;
                output_stream << "Operation Capacity: " << opt_cnt << std::endl;
                output_stream << "Op/Size Ratio: " << 1.0 * opt_cnt / table_size
                              << std::endl;
                output_stream << "CPU Time: " << duration << " ms" << std::endl;

                output_stream << "Throughput: "
                              << int(double(opt_cnt) / (duration / 1000.0))
                              << " ops/s" << std::endl;

                output_stream << "Latency: "
                              << int(duration * 1000000.0 / double(opt_cnt))
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
        case BenchmarkCaseType::YCSB_NEG_A:
        case BenchmarkCaseType::YCSB_B:
        case BenchmarkCaseType::YCSB_NEG_B:
        case BenchmarkCaseType::YCSB_C:
        case BenchmarkCaseType::YCSB_NEG_C:
            run = [this, para]() {
                std::vector<uint64_t> ycsb_keys;
                ycsb_load(ycsb_keys, para.ycsb_load_path, load_factor);

                BlastHT* available_keys;
                if (load_factor < 1.0) {
                    available_keys = new BlastHT(ycsb_keys.size() * 3, false);
                    for (const auto& key : ycsb_keys) {
                        available_keys->Insert(key, 0);
                    }
                }

                std::vector<std::pair<uint64_t, uint64_t>> ycsb_exe_vec;
                ycsb_exe_load(ycsb_exe_vec, para.ycsb_run_path, load_factor,
                              available_keys);

                auto start = std::chrono::high_resolution_clock::now();

                obj->YCSBFill(ycsb_keys, para.thread_num);

                auto start_fill = std::chrono::high_resolution_clock::now();
                auto fill_duration =
                    std::chrono::duration_cast<std::chrono::milliseconds>(
                        start_fill - start)
                        .count();

                obj->YCSBRun(ycsb_exe_vec, para.thread_num);

                auto start_run = std::chrono::high_resolution_clock::now();
                auto run_duration =
                    std::chrono::duration_cast<std::chrono::milliseconds>(
                        start_run - start_fill)
                        .count();

                auto end = std::chrono::high_resolution_clock::now();
                auto duration =
                    std::chrono::duration_cast<std::chrono::milliseconds>(end -
                                                                          start)
                        .count();

                int fill_op_cnt = ycsb_keys.size();
                int run_op_cnt = ycsb_exe_vec.size();

                output_stream << "CPU Time: " << duration << " ms" << std::endl;
                output_stream << "Fill Time: " << fill_duration << " ms"
                              << std::endl;
                output_stream << "Run Time: " << run_duration << " ms"
                              << std::endl;
                output_stream
                    << "Fill Latency: "
                    << int(fill_duration * 1000000.0 / double(fill_op_cnt))
                    << " ns/op" << std::endl;
                output_stream
                    << "Run Latency: "
                    << int(run_duration * 1000000.0 / double(run_op_cnt))
                    << " ns/op" << std::endl;
                output_stream
                    << "Fill Throughput: "
                    << int(double(fill_op_cnt) / (fill_duration / 1000.0))
                    << " ops/s" << std::endl;
                output_stream
                    << "Run Throughput: "
                    << int(double(run_op_cnt) / (run_duration / 1000.0))
                    << " ops/s" << std::endl;
            };
            break;
        case BenchmarkCaseType::YCSB_A_PERCENTILE:
        case BenchmarkCaseType::YCSB_NEG_A_PERCENTILE:
            run = [this, para]() {
                std::vector<uint64_t> ycsb_keys;
                ycsb_load(ycsb_keys, para.ycsb_load_path, load_factor);

                BlastHT* available_keys;

                if (load_factor < 1.0) {
                    available_keys = new BlastHT(ycsb_keys.size() * 3, false);
                    for (const auto& key : ycsb_keys) {
                        available_keys->Insert(key, 0);
                    }
                }

                std::vector<std::pair<uint64_t, uint64_t>> ycsb_exe_vec;
                ycsb_exe_load(ycsb_exe_vec, para.ycsb_run_path, load_factor,
                              available_keys);

                auto start = std::chrono::high_resolution_clock::now();

                obj->YCSBFill(ycsb_keys, para.thread_num);

                auto start_fill = std::chrono::high_resolution_clock::now();
                auto fill_duration =
                    std::chrono::duration_cast<std::chrono::milliseconds>(
                        start_fill - start)
                        .count();

                uint64_t record_num = 10000;

                // Define the percentiles to calculate
                std::vector<double> percentiles = {50.0, 95.0,  99.0,
                                                   99.9, 99.99, 100.0};

                auto latencies = obj->YCSBRunWithLatencyRecording(
                    ycsb_exe_vec, para.thread_num, record_num, percentiles);

                auto start_run = std::chrono::high_resolution_clock::now();
                auto run_duration =
                    std::chrono::duration_cast<std::chrono::milliseconds>(
                        start_run - start_fill)
                        .count();

                auto end = std::chrono::high_resolution_clock::now();
                auto duration =
                    std::chrono::duration_cast<std::chrono::milliseconds>(end -
                                                                          start)
                        .count();

                output_stream << "Percentile Latency Records: " << std::endl;
                output_stream << "Operation Type, Percentile, Latency (ns)"
                              << std::endl;
                for (auto& latency : latencies) {
                    output_stream << std::get<0>(latency) << ", "
                                  << std::get<1>(latency) << ", "
                                  << std::get<2>(latency) << std::endl;
                }
            };
            break;
        case BenchmarkCaseType::HASH_DISTRUBUTION:
            run = [this, para]() {
                auto* hash_dist = dynamic_cast<BenchmarkHashDistribution*>(obj);

                // Test different key generation strategies
                std::vector<std::pair<std::string, std::vector<uint64_t>>>
                    key_sets = {
                        {"Random Keys", hash_dist->Generate_Random_Keys(
                                            opt_num, para.thread_num)},
                        {"Sequential Keys", hash_dist->Generate_Sequential_Keys(
                                                opt_num, para.thread_num)},
                        {"Low Hamming Weight Keys",
                         hash_dist->Generate_Low_Hamming_Weight_Keys(
                             opt_num, para.thread_num)},
                        {"High Hamming Weight Keys",
                         hash_dist->Generate_High_Hamming_Weight_Keys(
                             opt_num, para.thread_num)}};

                for (const auto& key_set : key_sets) {
                    output_stream << "=== " << key_set.first
                                  << " ===" << std::endl;
                    hash_dist->Concurrent_Simulation(key_set.second,
                                                     para.thread_num);
                    auto res = hash_dist->Occupancy_Distribution();
                    output_stream
                        << "Operation Count: " << key_set.second.size()
                        << std::endl;
                    output_stream << "Bin Occupancy, Count: " << std::endl;
                    for (auto& i : res) {
                        output_stream << i.first << ", " << i.second
                                      << std::endl;
                    }
                    output_stream << std::endl;
                }
            };
            break;
        case BenchmarkCaseType::QUERY_HIT_CUSTOM_LOAD_FACTOR_ONLY_PERCENTILE:
            run = [this, para]() {
                std::vector<uint64_t> key_vec, value_vec;
                obj_fill_vec_prepare(key_vec, value_vec,
                                     int(floor(table_size * load_factor)));

                std::vector<std::tuple<uint64_t, uint64_t, uint64_t>> ops;
                if (thread_num) {
                    vec_to_ops(key_vec, value_vec, ops, ConcOptType::INSERT);
                }

                auto start = std::chrono::high_resolution_clock::now();

                if (thread_num) {
                    obj->ConcurrentRun(ops, thread_num);
                } else {
                    obj_fill(key_vec, value_vec);
                }

                auto end = std::chrono::high_resolution_clock::now();
                auto duration =
                    std::chrono::duration_cast<std::chrono::milliseconds>(end -
                                                                          start)
                        .count();

                output_stream << "Insert CPU Time: " << duration << " ms"
                              << std::endl;

                output_stream << "Insert Throughput: "
                              << int(double(table_size * load_factor) /
                                     (duration / 1000.0))
                              << " ops/s" << std::endl;

                output_stream << "Insert Latency: "
                              << int(duration * 1000000.0 /
                                     double(table_size * load_factor))
                              << " ns/op" << std::endl;

                std::vector<uint64_t> query_key_vec;
                std::vector<uint8_t> query_ptr_vec;
                batch_query_vec_prepare(key_vec, query_key_vec, opt_num, 1);

                if (thread_num) {
                    vec_to_ops(query_key_vec, ops, ConcOptType::QUERY);
                }

                uint64_t record_num = 10000;

                // Define the percentiles to calculate
                std::vector<double> percentiles = {50.0, 95.0,  99.0,
                                                   99.9, 99.99, 100.0};

                start = std::chrono::high_resolution_clock::now();

                auto latencies = obj->ConcurrentRunWithLatencyRecording(
                    ops, thread_num, record_num, percentiles);

                end = std::chrono::high_resolution_clock::now();
                duration =
                    std::chrono::duration_cast<std::chrono::milliseconds>(end -
                                                                          start)
                        .count();

                output_stream << "Query CPU Time: " << duration << " ms"
                              << std::endl;

                output_stream << "Query Throughput: "
                              << int(double(opt_num) / (duration / 1000.0))
                              << " ops/s" << std::endl;

                output_stream << "Query Latency: "
                              << int(duration * 1000000.0 / double(opt_num))
                              << " ns/op" << std::endl;

                output_stream << "Percentile Latency Records: " << std::endl;
                output_stream << "Operation Type, Percentile, Latency (ns)"
                              << std::endl;
                for (auto& latency : latencies) {
                    output_stream << std::get<0>(latency) << ", "
                                  << std::get<1>(latency) << ", "
                                  << std::get<2>(latency) << std::endl;
                }
            };
            break;
        case BenchmarkCaseType::MEMORY_MEASUREMENT_INSERTIONS:
            run = [this, para]() {
                // Pre-allocate vectors for memory measurements
                std::vector<uint64_t> memory_measurements;
                std::vector<uint64_t> operation_counts;
                memory_measurements.reserve(50);
                operation_counts.reserve(50);

                // Calculate the intervals for 50 even points
                uint64_t interval = opt_num / 50;
                uint64_t current_target = interval;

                // Function to get current max memory usage (VmHWM in KB)
                auto get_memory_usage = []() -> uint64_t {
                    std::ifstream status_file("/proc/self/status");
                    std::string line;
                    while (std::getline(status_file, line)) {
                        if (line.substr(0, 6) == "VmHWM:") {
                            // Extract the number (in KB)
                            std::istringstream iss(line.substr(6));
                            uint64_t memory_kb;
                            iss >> memory_kb;
                            return memory_kb;
                        }
                    }
                    return 0;
                };

                // Record initial memory
                uint64_t initial_memory = get_memory_usage();
                memory_measurements.push_back(initial_memory);
                operation_counts.push_back(0);

                auto start = std::chrono::high_resolution_clock::now();

                // Perform insertions and measure memory at intervals
                for (uint64_t i = 1; i <= opt_num; ++i) {
                    // Generate random key and value for memory-free insertion
                    uint64_t key = gen_key_hittable();
                    uint64_t value = gen_value();

                    // Perform the insertion
                    obj->Insert(key, value);

                    // Check if we've reached a measurement point
                    if (i == current_target || i == opt_num) {
                        uint64_t current_memory = get_memory_usage();
                        memory_measurements.push_back(current_memory);
                        operation_counts.push_back(i);

                        // Move to next target
                        if (current_target < opt_num) {
                            current_target += interval;
                            if (current_target > opt_num) {
                                current_target = opt_num;
                            }
                        }
                    }
                }

                auto end = std::chrono::high_resolution_clock::now();
                auto duration =
                    std::chrono::duration_cast<std::chrono::milliseconds>(end -
                                                                          start)
                        .count();

                // Output results
                output_stream << "Memory Measurement Along Insertions"
                              << std::endl;
                output_stream << "Total Operations: " << opt_num << std::endl;
                output_stream << "CPU Time: " << duration << " ms" << std::endl;
                output_stream << "Throughput: "
                              << int(double(opt_num) / (duration / 1000.0))
                              << " ops/s" << std::endl;
                output_stream << "Average Latency: "
                              << int(duration * 1000000.0 / double(opt_num))
                              << " ns/op" << std::endl;
                output_stream << std::endl;

                // Output memory measurements
                output_stream << "Memory Usage Measurements:" << std::endl;
                output_stream
                    << "Completion %, Memory Usage (MB), Memory Growth (MB)"
                    << std::endl;

                for (size_t i = 0; i < memory_measurements.size(); ++i) {
                    double completion_percent =
                        (double(operation_counts[i]) / opt_num) * 100.0;
                    double memory_mb =
                        memory_measurements[i] / 1024.0;  // Convert KB to MB
                    double memory_growth_mb =
                        (memory_measurements[i] - initial_memory) / 1024.0;
                    output_stream << std::fixed << std::setprecision(2)
                                  << completion_percent << ", " << std::fixed
                                  << std::setprecision(2) << memory_mb << ", "
                                  << std::fixed << std::setprecision(2)
                                  << memory_growth_mb << std::endl;
                }

                // Calculate and output memory efficiency metrics
                uint64_t max_memory = *std::max_element(
                    memory_measurements.begin(), memory_measurements.end());
                uint64_t total_memory_growth = max_memory - initial_memory;

                output_stream << std::endl;
                output_stream << "Memory Efficiency Metrics:" << std::endl;
                output_stream
                    << "Initial Memory: " << std::fixed << std::setprecision(2)
                    << (initial_memory / 1024.0) << " MB" << std::endl;
                output_stream << "Peak Memory: " << std::fixed
                              << std::setprecision(2) << (max_memory / 1024.0)
                              << " MB" << std::endl;
                output_stream << "Total Memory Growth: " << std::fixed
                              << std::setprecision(2)
                              << (total_memory_growth / 1024.0) << " MB"
                              << std::endl;
                output_stream << "Memory per Operation: " << std::fixed
                              << std::setprecision(2)
                              << (total_memory_growth / 1024.0) / opt_num
                              << " MB/op" << std::endl;
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
