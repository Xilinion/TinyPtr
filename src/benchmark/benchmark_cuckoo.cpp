#include "benchmark_cuckoo.h"
#include <cstdint>

namespace tinyptr {

const BenchmarkObjectType BenchmarkCuckoo::TYPE = BenchmarkObjectType::CUCKOO;

BenchmarkCuckoo::BenchmarkCuckoo(int n) : BenchmarkObject64(TYPE) {
    tab = new libcuckoo::cuckoohash_map<uint64_t, uint64_t>(n);
}

uint8_t BenchmarkCuckoo::Insert(uint64_t key, uint64_t value) {
    return tab->insert(key, value);
}

uint64_t BenchmarkCuckoo::Query(uint64_t key, uint8_t ptr) {
    uint64_t value;
    tab->find(key, value);
    return value;
}

void BenchmarkCuckoo::Update(uint64_t key, uint8_t ptr, uint64_t value) {
    tab->update(key, value);
}

void BenchmarkCuckoo::Erase(uint64_t key, uint8_t ptr) {
    tab->erase(key);
}

void BenchmarkCuckoo::YCSBFill(std::vector<uint64_t>& keys, int num_threads) {
    std::vector<std::thread> threads;
    size_t chunk_size = keys.size() / num_threads;

    for (int i = 0; i < num_threads; ++i) {
        size_t start_index = i * chunk_size;
        size_t end_index =
            (i == num_threads - 1) ? keys.size() : start_index + chunk_size;

        threads.emplace_back([this, &keys, start_index, end_index]() {
            for (size_t j = start_index; j < end_index; ++j) {
                tab->insert(keys[j], 0);
            }
        });
    }

    for (auto& thread : threads) {
        thread.join();
    }
}

void BenchmarkCuckoo::YCSBRun(std::vector<std::pair<uint64_t, uint64_t>>& ops,
                              int num_threads) {
    std::vector<std::thread> threads;
    size_t chunk_size = ops.size() / num_threads;

    for (int i = 0; i < num_threads; ++i) {
        size_t start_index = i * chunk_size;
        size_t end_index =
            (i == num_threads - 1) ? ops.size() : start_index + chunk_size;

        threads.emplace_back([this, &ops, start_index, end_index]() {
            uint64_t value;
            for (size_t j = start_index; j < end_index; ++j) {
                if (ops[j].first == 1) {
                    tab->insert(ops[j].second, 0);
                } else {
                    tab->find(ops[j].second, value);
                }
            }
        });
    }

    for (auto& thread : threads) {
        thread.join();
    }
}

void BenchmarkCuckoo::ConcurrentRun(
    std::vector<std::tuple<uint64_t, uint64_t, uint64_t>>& ops,
    int num_threads) {
    std::vector<std::thread> threads;
    size_t chunk_size = ops.size() / num_threads;

    for (int i = 0; i < num_threads; ++i) {
        size_t start_index = i * chunk_size;
        size_t end_index =
            (i == num_threads - 1) ? ops.size() : start_index + chunk_size;

        threads.emplace_back([this, &ops, start_index, end_index]() {
            for (size_t j = start_index; j < end_index; ++j) {
                if (std::get<0>(ops[j]) == 0) {
                    tab->insert(std::get<1>(ops[j]), std::get<2>(ops[j]));
                } else if (std::get<0>(ops[j]) == 1) {
                    tab->find(std::get<1>(ops[j]), std::get<2>(ops[j]));
                } else if (std::get<0>(ops[j]) == 2) {
                    tab->update(std::get<1>(ops[j]), std::get<2>(ops[j]));
                } else if (std::get<0>(ops[j]) == 3) {
                    tab->erase(std::get<1>(ops[j]));
                }
            }
        });
    }

    for (auto& thread : threads) {
        thread.join();
    }
}

}  // namespace tinyptr