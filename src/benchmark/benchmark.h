#pragma once

#include <fstream>
#include <functional>
#include <iostream>
#include <string>
#include "benchmark_cli_para.h"

class Benchmark {
   private:
    enum class CaseIndicator : int {
        LOAD_FACTOR_INSERT_ONLY_RAND = 0,
        THROUGHPUT_INSERT_ONLY_RAND,
        COUNT
    };

    enum class ObjectIndicator : int { DEREFTAB64 = 0, CHAINEDHT64 = 1, COUNT };

   public:
    static constexpr int BenchCaseNum = static_cast<int>(CaseIndicator::COUNT);
    static constexpr int BenchObjNum = static_cast<int>(ObjectIndicator::COUNT);

   public:
    Benchmark(BenchmarkCLIPara& para);
    ~Benchmark() = default;

    void Run();

   private:
    std::function<void()> run;
    std::ofstream output_stream;
};