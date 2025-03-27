#include "benchmark_resizable_skulkerht.h"

namespace tinyptr {

const BenchmarkObjectType BenchmarkResizableSkulkerHT::TYPE =
    BenchmarkObjectType::RESIZABLE_SKULKERHT;

BenchmarkResizableSkulkerHT::BenchmarkResizableSkulkerHT(
    uint64_t initial_size_per_part_, uint64_t part_num_, uint32_t thread_num_,
    double resize_threshold_, double resize_factor_)
    : BenchmarkObject64(TYPE) {
    tab = new ResizableSkulkerHT(initial_size_per_part_, part_num_, thread_num_,
                                 resize_threshold_, resize_factor_);
}

uint8_t BenchmarkResizableSkulkerHT::Insert(uint64_t key, uint64_t value) {
    return 0;
}

uint64_t BenchmarkResizableSkulkerHT::Query(uint64_t key, uint8_t ptr) {
    return 0;
}

void BenchmarkResizableSkulkerHT::Update(uint64_t key, uint8_t ptr,
                                         uint64_t value) {
    return;
}

void BenchmarkResizableSkulkerHT::Erase(uint64_t key, uint8_t ptr) {
    return;
}

void BenchmarkResizableSkulkerHT::YCSBFill(std::vector<uint64_t>& keys,
                                           int num_threads) {
    std::vector<std::thread> threads;
    size_t chunk_size = keys.size() / num_threads;

    for (int i = 0; i < num_threads; ++i) {
        size_t start_index = i * chunk_size;
        size_t end_index =
            (i == num_threads - 1) ? keys.size() : start_index + chunk_size;

        threads.emplace_back([this, &keys, start_index, end_index]() {
            uint64_t handle = tab->GetHandle();
            for (size_t j = start_index; j < end_index; ++j) {
                tab->Insert(handle, keys[j], 0);
            }
            tab->FreeHandle(handle);
        });
    }

    for (auto& thread : threads) {
        thread.join();
    }
}

void BenchmarkResizableSkulkerHT::YCSBRun(
    std::vector<std::pair<uint64_t, uint64_t>>& ops, int num_threads) {
    std::vector<std::thread> threads;
    size_t chunk_size = ops.size() / num_threads;

    for (int i = 0; i < num_threads; ++i) {
        size_t start_index = i * chunk_size;
        size_t end_index =
            (i == num_threads - 1) ? ops.size() : start_index + chunk_size;

        threads.emplace_back([this, &ops, start_index, end_index]() {
            uint64_t handle = tab->GetHandle();
            uint64_t value;
            for (size_t j = start_index; j < end_index; ++j) {
                if (ops[j].first == 1) {
                    tab->Insert(handle, ops[j].second, 0);
                } else {
                    tab->Query(handle, ops[j].second, &value);
                }
            }
            tab->FreeHandle(handle);
        });
    }

    for (auto& thread : threads) {
        thread.join();
    }
}

void BenchmarkResizableSkulkerHT::ConcurrentRun(
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
            uint64_t handle = tab->GetHandle();
            for (size_t j = start_index; j < end_index; ++j) {
                if (std::get<0>(ops[j]) == 0) {
                    tab->Insert(handle, std::get<1>(ops[j]),
                                std::get<2>(ops[j]));
                } else if (std::get<0>(ops[j]) == 1) {
                    tab->Query(handle, std::get<1>(ops[j]), &value);
                } else if (std::get<0>(ops[j]) == 2) {
                    tab->Update(handle, std::get<1>(ops[j]),
                                std::get<2>(ops[j]));
                } else if (std::get<0>(ops[j]) == 3) {
                    tab->Erase(handle, std::get<1>(ops[j]));
                }
            }
            tab->FreeHandle(handle);
        });
    }

    for (auto& thread : threads) {
        thread.join();
    }
}

}  // namespace tinyptr
