#include "benchmark_iceberg.h"
#include <pthread.h>
#include <fstream>
#include <thread>
#include "unistd.h"

namespace tinyptr {

const BenchmarkObjectType BenchmarkIceberg::TYPE = BenchmarkObjectType::ICEBERG;

uint64_t BenchmarkIceberg::auto_iceberg_slot_num(uint64_t size) {
    uint64_t res = 0;
    while (size) {
        size >>= 1;
        res++;
    }
    return res;
}

BenchmarkIceberg::BenchmarkIceberg(int n) : BenchmarkObject64(TYPE) {
    iceberg_init(&tab, auto_iceberg_slot_num(n));
    tid = gettid();
}

uint8_t BenchmarkIceberg::Insert(uint64_t key, uint64_t value) {
    return iceberg_insert(&tab, key, value, tid);
}

uint64_t BenchmarkIceberg::Query(uint64_t key, uint8_t ptr) {
    uint64_t res;
    iceberg_get_value(&tab, key, &res, tid);
    return res;
}

void BenchmarkIceberg::Update(uint64_t key, uint8_t ptr, uint64_t value) {
    iceberg_insert(&tab, key, value, tid);
}

void BenchmarkIceberg::Erase(uint64_t key, uint8_t ptr) {
    iceberg_remove(&tab, key, tid);
}

}  // namespace tinyptr