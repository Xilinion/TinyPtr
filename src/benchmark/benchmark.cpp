#include "benchmark.h"
#include "benchmark_cli_para.h"
#include "chained_ht_64.h"
#include "dereference_table_64.h"

namespace tinyptr {

Benchmark::Benchmark(BenchmarkCLIPara& para)
    : output_stream(para.GetOuputFileName()) {
    switch (static_cast<BenchmarkCaseType>(para.case_id)) {
        case BenchmarkCaseType::INSERT_ONLY_LOAD_FACTOR_SUPPORT:
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
}  // namespace tinyptr