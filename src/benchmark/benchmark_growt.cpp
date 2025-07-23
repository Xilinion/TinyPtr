#if 0

#include "benchmark_growt.h"

namespace tinyptr {

const BenchmarkObjectType BenchmarkGrowt::TYPE = BenchmarkObjectType::GROWT;

BenchmarkGrowt::BenchmarkGrowt(int n) : BenchmarkObject64(TYPE), ht(n) {}

// Wrapper function definitions
[[gnu::noinline]] void BenchmarkGrowt::insert_wrapper(uint64_t k, uint64_t v) {
    ht.get_handle().insert(k, v);
}

[[gnu::noinline]] uint64_t BenchmarkGrowt::query_wrapper(uint64_t k) {
    auto it = ht.get_handle().find(k);
    if (it == ht.get_handle().end()) {
        return 0;
    }
    return (*it).second;
}

[[gnu::noinline]] void BenchmarkGrowt::update_wrapper(uint64_t k, uint64_t v) {
    ht.get_handle().update(k, [v](uint64_t& lhs) { return (lhs = v); });
}

[[gnu::noinline]] void BenchmarkGrowt::erase_wrapper(uint64_t k) {
    ht.get_handle().erase(k);
}

uint8_t BenchmarkGrowt::Insert(uint64_t key, uint64_t value) {
    static uint64_t insert_count = 0;
    insert_count++;
    insert_wrapper(insert_count, insert_count);
    // insert_wrapper(key, value);
    return 0;
}

uint64_t BenchmarkGrowt::Query(uint64_t key, uint8_t ptr) {
    return query_wrapper(key);
}

void BenchmarkGrowt::Update(uint64_t key, uint8_t ptr, uint64_t value) {
    update_wrapper(key, value);
}

void BenchmarkGrowt::Erase(uint64_t key, uint8_t ptr) {
    erase_wrapper(key);
}
}  // namespace tinyptr
#endif