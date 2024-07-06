#include "benchmark_intarray64.h"
#include "benchmark_object_64.h"

namespace tinyptr {

BenchmarkIntArray64::BenchmarkIntArray64(int n)
    : BenchmarkObject64(TYPE), tab_size(n) {
    tab = new uint64_t[n];
}

uint8_t BenchmarkIntArray64::Insert(uint64_t key, uint64_t value) {
    tab[key % tab_size] = value;
    return 0;
}

uint64_t BenchmarkIntArray64::Query(uint64_t key, uint8_t ptr) {
    return tab[key % tab_size];
}

void BenchmarkIntArray64::Update(uint64_t key, uint8_t ptr, uint64_t value) {
    tab[key % tab_size] = value;
}

void BenchmarkIntArray64::Erase(uint64_t key, uint8_t ptr) {
    tab[key % tab_size] = 0;
}

}  // namespace tinyptr