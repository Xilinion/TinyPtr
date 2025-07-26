#include "benchmark_clht.h"
#include <fstream>
#include "unistd.h"

namespace tinyptr {

const BenchmarkObjectType BenchmarkCLHT::TYPE = BenchmarkObjectType::CLHT;

BenchmarkCLHT::BenchmarkCLHT(int n) : BenchmarkObject64(TYPE) {
    tab = clht_create(n >> 1);
    clht_gc_thread_init(tab, 0);
}

BenchmarkCLHT::~BenchmarkCLHT() {
    clht_gc_destroy(tab);
}

uint8_t BenchmarkCLHT::Insert(uint64_t key, uint64_t value) {
    return clht_put(tab, key, value);
}

uint64_t BenchmarkCLHT::Query(uint64_t key, uint8_t ptr) {
    return clht_get(tab->ht, key);
}

void BenchmarkCLHT::Update(uint64_t key, uint8_t ptr, uint64_t value) {
    clht_put(tab, key, value);
}

void BenchmarkCLHT::Erase(uint64_t key, uint8_t ptr) {
    clht_remove(tab, key);
}

void BenchmarkCLHT::YCSBFill(std::vector<uint64_t>& keys, int num_threads) {
    std::vector<std::thread> threads;
    size_t chunk_size = keys.size() / num_threads;

    for (int i = 0; i < num_threads; ++i) {
        size_t start_index = i * chunk_size;
        size_t end_index =
            (i == num_threads - 1) ? keys.size() : start_index + chunk_size;

        threads.emplace_back([this, &keys, start_index, end_index, i]() {
            clht_gc_thread_init(tab, i + 1);
            for (size_t j = start_index; j < end_index; ++j) {
                clht_put(tab, keys[j], 0);
            }
        });
    }

    for (auto& thread : threads) {
        thread.join();
    }

    // clht_gc_destroy(tab);
}

void BenchmarkCLHT::YCSBRun(std::vector<std::pair<uint64_t, uint64_t>>& ops,
                            int num_threads) {
    std::vector<std::thread> threads;
    size_t chunk_size = ops.size() / num_threads;

    for (int i = 0; i < num_threads; ++i) {
        size_t start_index = i * chunk_size;
        size_t end_index =
            (i == num_threads - 1) ? ops.size() : start_index + chunk_size;

        threads.emplace_back([this, &ops, start_index, end_index, i]() {
            // clht_gc_thread_init(tab, gettid());
            for (size_t j = start_index; j < end_index; ++j) {
                if (ops[j].first == 1) {
                    clht_put(tab, ops[j].second, 0);
                } else {
                    clht_get(tab->ht, ops[j].second);
                }
            }
        });
    }

    for (auto& thread : threads) {
        thread.join();
    }

    clht_gc_destroy(tab);
}

void BenchmarkCLHT::ConcurrentRun(
    std::vector<std::tuple<uint64_t, uint64_t, uint64_t>>& ops,
    int num_threads) {
    std::vector<std::thread> threads;
    size_t chunk_size = ops.size() / num_threads;

    for (int i = 0; i < num_threads; ++i) {
        size_t start_index = i * chunk_size;
        size_t end_index =
            (i == num_threads - 1) ? ops.size() : start_index + chunk_size;

        threads.emplace_back([this, &ops, start_index, end_index, i]() {
            clht_gc_thread_init(tab, i + 1);
            uint64_t value;
            for (size_t j = start_index; j < end_index; ++j) {
                if (std::get<0>(ops[j]) == ConcOptType::INSERT) {
                    clht_put(tab, std::get<1>(ops[j]), std::get<2>(ops[j]));
                } else if (std::get<0>(ops[j]) == ConcOptType::QUERY) {
                    clht_get(tab->ht, std::get<1>(ops[j]));
                } else if (std::get<0>(ops[j]) == ConcOptType::UPDATE) {
                    clht_put(tab, std::get<1>(ops[j]), std::get<2>(ops[j]));
                } else if (std::get<0>(ops[j]) == ConcOptType::ERASE) {
                    clht_remove(tab, std::get<1>(ops[j]));
                }
            }
        });
    }

    for (auto& thread : threads) {
        thread.join();
    }

    clht_gc_destroy(tab);
}

}  // namespace tinyptr