#include "benchmark_junction.h"

namespace tinyptr {

const BenchmarkObjectType BenchmarkJunction::TYPE =
    BenchmarkObjectType::JUNCTION;

BenchmarkJunction::BenchmarkJunction(int n) : BenchmarkObject64(TYPE) {
    tab = new junction::ConcurrentMap_Grampa<uint64_t, uint64_t>;
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

}  // namespace tinyptr