#include "benchmark_junction.h"
#include <cstdint>

namespace tinyptr {

const BenchmarkObjectType BenchmarkJunction::TYPE =
    BenchmarkObjectType::JUNCTION;

BenchmarkJunction::BenchmarkJunction(int n) : BenchmarkObject64(TYPE) {
    uint64_t table_size = 1;
    while (table_size < n) {
        table_size *= 2;
    }
    // tab = new junction::ConcurrentMap_Grampa<uint64_t, uint64_t>(n);
    tab = new junction::ConcurrentMap_Leapfrog<uint64_t, uint64_t>(table_size);
}

uint8_t BenchmarkJunction::Insert(uint64_t key, uint64_t value) {
    tab->assign(key, value);
    return 0;
}

uint64_t BenchmarkJunction::Query(uint64_t key, uint8_t ptr) {
    return tab->get(key);
}

void BenchmarkJunction::Update(uint64_t key, uint8_t ptr, uint64_t value) {
    tab->exchange(key, value);
}

void BenchmarkJunction::Erase(uint64_t key, uint8_t ptr) {
    tab->erase(key);
}

void BenchmarkJunction::YCSBFill(std::vector<uint64_t>& keys, int num_threads) {
    std::vector<std::thread> threads;
    size_t chunk_size = keys.size() / num_threads;

    junction::QSBR::Context context = junction::DefaultQSBR.createContext();

    for (int i = 0; i < num_threads; ++i) {
        size_t start_index = i * chunk_size;
        size_t end_index =
            (i == num_threads - 1) ? keys.size() : start_index + chunk_size;

        threads.emplace_back(
            [this, &keys, start_index, end_index, i, context]() {
                for (size_t j = start_index; j < end_index; ++j) {
                    tab->assign(keys[j], 0);
                    if (j & ((1 << 10) - 1) == 0) {
                        junction::DefaultQSBR.update(context);
                    }
                }
            });
    }

    for (auto& thread : threads) {
        thread.join();
    }

    junction::DefaultQSBR.destroyContext(context);
}

void BenchmarkJunction::YCSBRun(std::vector<std::pair<uint64_t, uint64_t>>& ops,
                                int num_threads) {
    std::vector<std::thread> threads;
    size_t chunk_size = ops.size() / num_threads;

    junction::QSBR::Context context = junction::DefaultQSBR.createContext();

    for (int i = 0; i < num_threads; ++i) {
        size_t start_index = i * chunk_size;
        size_t end_index =
            (i == num_threads - 1) ? ops.size() : start_index + chunk_size;

        threads.emplace_back(
            [this, &ops, start_index, end_index, i, context]() {
                for (size_t j = start_index; j < end_index; ++j) {
                    if (ops[j].first == 1) {
                        tab->assign(ops[j].second, 0);
                    } else {
                        tab->get(ops[j].second);
                    }

                    if (j & ((1 << 10) - 1) == 0) {
                        junction::DefaultQSBR.update(context);
                    }
                }
            });
    }

    for (auto& thread : threads) {
        thread.join();
    }
    junction::DefaultQSBR.destroyContext(context);
}

void BenchmarkJunction::ConcurrentRun(
    std::vector<std::tuple<uint64_t, uint64_t, uint64_t>>& ops,
    int num_threads) {
    std::vector<std::thread> threads;
    size_t chunk_size = ops.size() / num_threads;

    junction::QSBR::Context context = junction::DefaultQSBR.createContext();

    for (int i = 0; i < num_threads; ++i) {
        size_t start_index = i * chunk_size;
        size_t end_index =
            (i == num_threads - 1) ? ops.size() : start_index + chunk_size;

        threads.emplace_back(
            [this, &ops, start_index, end_index, i, context]() {
                for (size_t j = start_index; j < end_index; ++j) {
                    if (std::get<0>(ops[j]) == ConcOptType::INSERT) {
                        tab->assign(std::get<1>(ops[j]), std::get<2>(ops[j]));
                    } else if (std::get<0>(ops[j]) == ConcOptType::QUERY) {
                        tab->get(std::get<1>(ops[j]));
                    } else if (std::get<0>(ops[j]) == ConcOptType::UPDATE) {
                        tab->exchange(std::get<1>(ops[j]), std::get<2>(ops[j]));
                    } else if (std::get<0>(ops[j]) == ConcOptType::ERASE) {
                        tab->erase(std::get<1>(ops[j]));
                    }

                    if (j & ((1 << 10) - 1) == 0) {
                        junction::DefaultQSBR.update(context);
                    }
                }
            });
    }

    for (auto& thread : threads) {
        thread.join();
    }

    junction::DefaultQSBR.destroyContext(context);
}

}  // namespace tinyptr