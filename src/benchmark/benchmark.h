#pragma once

#include <fstream>
#include <functional>
#include <iostream>
#include <string>
#include "benchmark_case_type.h"
#include "benchmark_cli_para.h"
#include "benchmark_object_type.h"

namespace tinyptr {

class Benchmark {
   public:
    Benchmark(BenchmarkCLIPara& para);
    ~Benchmark() = default;

    void Run();

   private:
    std::function<void()> run;
    std::ofstream output_stream;
};
}  // namespace tinyptr