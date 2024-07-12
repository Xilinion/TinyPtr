#include "benchmark_chainedht64.h"

namespace tinyptr {

const BenchmarkObjectType BenchmarkChainedHT64::TYPE =
    BenchmarkObjectType::CHAINEDHT64;

BenchmarkChainedHT64::BenchmarkChainedHT64(int n) : BenchmarkObject64(TYPE) {
    tab = new ChainedHT64(n);
}

uint8_t BenchmarkChainedHT64::Insert(uint64_t key, uint64_t value) {
    tab->Insert(key, value);
    return 0;
}

uint64_t BenchmarkChainedHT64::Query(uint64_t key, uint8_t ptr) {
    return tab->Query(key);
}

void BenchmarkChainedHT64::Update(uint64_t key, uint8_t ptr, uint64_t value) {
    tab->Update(key, value);
}

void BenchmarkChainedHT64::Erase(uint64_t key, uint8_t ptr) {
    tab->Erase(key);
}

}  // namespace tinyptr