#include "benchmark/benchmark_nonconc_blast_ht.h"
#include "benchmark_blast_ht.h"
#include <thread>
#include <cassert>
#include <vector>
#include "blast_ht.h"
#include "nonconc_blast_ht.h"

namespace tinyptr {

const BenchmarkObjectType BenchmarkNonConcBlastHT::TYPE = BenchmarkObjectType::NONCONC_BLAST;

BenchmarkNonConcBlastHT::BenchmarkNonConcBlastHT(uint64_t size, uint16_t bin_size)
    : BenchmarkObject64(TYPE) {
    tab = new NonConcBlastHT(size, bin_size, false);
}

uint8_t BenchmarkNonConcBlastHT::Insert(uint64_t key, uint64_t value) {
    if (tab->Insert(key, value)) {
        return 1;
    }
    return ~0;
}

uint64_t BenchmarkNonConcBlastHT::Query(uint64_t key, uint8_t ptr) {
    uint64_t value;
    tab->Query(key, &value);
    return value;
}

void BenchmarkNonConcBlastHT::Update(uint64_t key, uint8_t ptr, uint64_t value) {
    tab->Update(key, value);
}

void BenchmarkNonConcBlastHT::Erase(uint64_t key, uint8_t ptr) {
    tab->Free(key);
}
void BenchmarkNonConcBlastHT::ConcurrentRun(
    std::vector<std::tuple<uint64_t, uint64_t, uint64_t>>& ops,
    int num_threads) {
    assert(num_threads == 1);
    std::vector<std::thread> threads;
    size_t chunk_size = ops.size() / num_threads;
    for (int i = 0; i < num_threads; ++i) {
        size_t start_index = i * chunk_size;
        size_t end_index =
            (i == num_threads - 1) ? ops.size() : start_index + chunk_size;

        threads.emplace_back([this, &ops, start_index, end_index, i]() {
            uint64_t value;
            for (size_t j = start_index; j < end_index; ++j) {
                if (std::get<0>(ops[j]) == ConcOptType::INSERT) {
                    tab->Insert(std::get<1>(ops[j]), std::get<2>(ops[j]));
                } else if (std::get<0>(ops[j]) == ConcOptType::QUERY) {
                    tab->Query(std::get<1>(ops[j]), &value);
                } else if (std::get<0>(ops[j]) == ConcOptType::UPDATE) {
                    tab->Update(std::get<1>(ops[j]), std::get<2>(ops[j]));
                } else if (std::get<0>(ops[j]) == ConcOptType::ERASE) {
                    tab->Free(std::get<1>(ops[j]));
                }
            }
        });
    }

    for (auto& thread : threads) {
        thread.join();
    }
}

void BenchmarkNonConcBlastHT::Stats() {
    tab->Scan4Stats();
}

}  // namespace tinyptr
