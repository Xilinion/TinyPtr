#include "benchmark.h"
#include "benchmark_cli_para.h"
#include "chained_ht_64.h"
#include "dereference_table_64.h"

using namespace tinyptr;

Benchmark::Benchmark(BenchmarkCLIPara& para)
    : output_stream(para.GetOuputFileName()) {
    switch (para.case_id) {
        case static_cast<int>(CaseIndicator::LOAD_FACTOR_INSERT_ONLY_RAND):
            run = [this]() {
            };
            break;
        default:
            abort();
    }
}

void Benchmark::Run() {
    this->run();
    int n = 1e6, m = 1e6;
    DereferenceTable64 deref_tab(m);

    auto key = new int[n];

    auto ptr = new int[n];

    auto value = new int[n];

    for (int i = 0; i < n; ++i) {
        key[i] = rand(), value[i] = rand();
        ptr[i] = deref_tab.Allocate(key[i], value[i]);
    }
    for (int i = 0; i < n; ++i) {
        uint64_t tmp;
        deref_tab.Query(key[i], ptr[i], &tmp);
        assert(tmp == value[i]);
    }
}