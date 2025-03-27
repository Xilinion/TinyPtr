#include "benchmark_iceberg.h"
#include <pthread.h>
#include <fstream>
#include <thread>
#include "unistd.h"

namespace tinyptr {

const BenchmarkObjectType BenchmarkIceberg::TYPE = BenchmarkObjectType::ICEBERG;

uint64_t BenchmarkIceberg::auto_iceberg_slot_num(uint64_t size) {
    uint64_t res = 0;
    while (size) {
        size >>= 1;
        res++;
    }
    return res;
}

BenchmarkIceberg::BenchmarkIceberg(int n) : BenchmarkObject64(TYPE) {
    iceberg_init(&tab, auto_iceberg_slot_num(n));
    tid = gettid();
}

uint8_t BenchmarkIceberg::Insert(uint64_t key, uint64_t value) {
    return iceberg_insert(&tab, key, value, tid);
}

uint64_t BenchmarkIceberg::Query(uint64_t key, uint8_t ptr) {
    uint64_t res;
    iceberg_get_value(&tab, key, &res, tid);
    return res;
}

void BenchmarkIceberg::Update(uint64_t key, uint8_t ptr, uint64_t value) {
    iceberg_insert(&tab, key, value, tid);
}

void BenchmarkIceberg::Erase(uint64_t key, uint8_t ptr) {
    iceberg_remove(&tab, key, tid);
}

void BenchmarkIceberg::YCSBFill(std::vector<uint64_t>& keys, int num_threads) {
    std::vector<std::thread> threads;
    size_t chunk_size = keys.size() / num_threads;

    for (int i = 0; i < num_threads; ++i) {
        size_t start_index = i * chunk_size;
        size_t end_index =
            (i == num_threads - 1) ? keys.size() : start_index + chunk_size;

        threads.emplace_back([this, &keys, start_index, end_index, i]() {
            for (size_t j = start_index; j < end_index; ++j) {
                iceberg_insert(&tab, keys[j], 0, i);
            }
        });
    }

    for (auto& thread : threads) {
        thread.join();
    }
}

void BenchmarkIceberg::YCSBRun(std::vector<std::pair<uint64_t, uint64_t>>& ops,
                               int num_threads) {
    std::vector<std::thread> threads;
    size_t chunk_size = ops.size() / num_threads;

    for (int i = 0; i < num_threads; ++i) {
        size_t start_index = i * chunk_size;
        size_t end_index =
            (i == num_threads - 1) ? ops.size() : start_index + chunk_size;

        threads.emplace_back([this, &ops, start_index, end_index, i]() {
            uint64_t value;
            for (size_t j = start_index; j < end_index; ++j) {
                if (ops[j].first == 1) {
                    iceberg_insert(&tab, ops[j].second, 0, i);
                } else {
                    iceberg_get_value(&tab, ops[j].second, &value, i);
                }
            }
        });
    }

    for (auto& thread : threads) {
        thread.join();
    }
}

void BenchmarkIceberg::ConcurrentRun(
    std::vector<std::tuple<uint64_t, uint64_t, uint64_t>>& ops,
    int num_threads) {
    std::vector<std::thread> threads;
    size_t chunk_size = ops.size() / num_threads;

    for (int i = 0; i < num_threads; ++i) {
        size_t start_index = i * chunk_size;
        size_t end_index =
            (i == num_threads - 1) ? ops.size() : start_index + chunk_size;

        threads.emplace_back([this, &ops, start_index, end_index, i]() {
            uint64_t value;
            for (size_t j = start_index; j < end_index; ++j) {
                if (std::get<0>(ops[j]) == 0) {
                    iceberg_insert(&tab, std::get<1>(ops[j]),
                                   std::get<2>(ops[j]), i);
                } else if (std::get<0>(ops[j]) == 1) {
                    iceberg_get_value(&tab, std::get<1>(ops[j]), &value, i);
                } else if (std::get<0>(ops[j]) == 2) {
                    iceberg_insert(&tab, std::get<1>(ops[j]),
                                   std::get<2>(ops[j]), i);
                } else if (std::get<0>(ops[j]) == 3) {
                    iceberg_remove(&tab, std::get<1>(ops[j]), i);
                }
            }
        });
    }

    for (auto& thread : threads) {
        thread.join();
    }
}

}  // namespace tinyptr