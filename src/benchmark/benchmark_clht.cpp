#include "benchmark_clht.h"
#include <fstream>
#include "unistd.h"

namespace tinyptr {

const BenchmarkObjectType BenchmarkCLHT::TYPE = BenchmarkObjectType::CLHT;

BenchmarkCLHT::BenchmarkCLHT(int n) : BenchmarkObject64(TYPE) {
    tab = clht_create(n >> 1);
    clht_gc_thread_init(tab, gettid());
}

BenchmarkCLHT::~BenchmarkCLHT() {
    clht_gc_destroy(tab);
}

uint8_t BenchmarkCLHT::Insert(uint64_t key, uint64_t value) {
    return clht_put(tab, key, value);
}

uint64_t BenchmarkCLHT::Query(uint64_t key, uint8_t ptr) {
    return clht_get(tab->ht, key);
}

void BenchmarkCLHT::Update(uint64_t key, uint8_t ptr, uint64_t value) {
    clht_put(tab, key, value);
}

void BenchmarkCLHT::Erase(uint64_t key, uint8_t ptr) {
    clht_remove(tab, key);
}

}  // namespace tinyptr