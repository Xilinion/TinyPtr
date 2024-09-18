#include "benchmark_cuckoo.h"

namespace tinyptr {

const BenchmarkObjectType BenchmarkCuckoo::TYPE = BenchmarkObjectType::CUCKOO;

BenchmarkCuckoo::BenchmarkCuckoo(int n) : BenchmarkObject64(TYPE) {}

uint8_t BenchmarkCuckoo::Insert(uint64_t key, uint64_t value) {
    return tab.insert(key, value);
}

uint64_t BenchmarkCuckoo::Query(uint64_t key, uint8_t ptr) {
    return tab.find(key);
}

void BenchmarkCuckoo::Update(uint64_t key, uint8_t ptr, uint64_t value) {
    tab.update(key, value);
}

void BenchmarkCuckoo::Erase(uint64_t key, uint8_t ptr) {
    tab.erase(key);
}

}  // namespace tinyptr