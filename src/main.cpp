#pragma warning "Ignore '#pragma once' in main file"

#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <map>
#include <string>
#include <utility>
#include "benchmark/benchmark.h"
#include "benchmark/benchmark_cli_para.h"
#include "dereference_table_64.h"
#include "xxhash64.h"

int main(int argc, char** argv) {

    BenchmarkCLIPara para;
    para.Parse(argc, argv);
    Benchmark b(para);
    b.Run();

    return 0;
}