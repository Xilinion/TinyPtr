#include "benchmark_std_unordered_map_64.h"

namespace tinyptr {

const BenchmarkObjectType BenchmarkStdUnorderedMap64::TYPE =
    BenchmarkObjectType::STDUNORDEREDMAP64;

BenchmarkStdUnorderedMap64::BenchmarkStdUnorderedMap64(int n)
    : BenchmarkObject64(TYPE) {
    ht.reserve(n);
}

uint8_t BenchmarkStdUnorderedMap64::Insert(uint64_t key, uint64_t value) {
    ht.emplace(key, value);
    return 0;
}

uint64_t BenchmarkStdUnorderedMap64::Query(uint64_t key, uint8_t ptr) {
    return ht[key];
}

void BenchmarkStdUnorderedMap64::Update(uint64_t key, uint8_t ptr,
                                        uint64_t value) {
    ht[key] = value;
}

void BenchmarkStdUnorderedMap64::Erase(uint64_t key, uint8_t ptr) {
    ht.erase(key);
}
}  // namespace tinyptr