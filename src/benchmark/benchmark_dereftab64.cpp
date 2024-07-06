#include "benchmark_dereftab64.h"

namespace tinyptr {

const BenchmarkObjectType BenchmarkDerefTab64::TYPE =
    BenchmarkObjectType::DEREFTAB64;

BenchmarkDerefTab64::BenchmarkDerefTab64(int n) : BenchmarkObject64(TYPE) {
    tab = new DereferenceTable64(n);
}

uint8_t BenchmarkDerefTab64::Insert(uint64_t key, uint64_t value) {
    return tab->Allocate(key, value);
}

uint64_t BenchmarkDerefTab64::Query(uint64_t key, uint8_t ptr) {
    uint64_t res;
    tab->Query(key, ptr, &res);
    return res;
}

void BenchmarkDerefTab64::Update(uint64_t key, uint8_t ptr, uint64_t value) {
    tab->Update(key, ptr, value);
}

void BenchmarkDerefTab64::Erase(uint64_t key, uint8_t ptr) {
    tab->Free(key, ptr);
}

}  // namespace tinyptr