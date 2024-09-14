#pragma once

#include <cstdlib>
#include <string>
#include "benchmark_case_type.h"
#include "benchmark_object_type.h"

namespace tinyptr {

class BenchmarkCLIPara {
   private:
    static void configuring_getopt();

   public:
    BenchmarkCLIPara() = default;
    ~BenchmarkCLIPara() = default;

    void Parse(int argc, char** argv);
    std::string GetOuputFileName();

   public:
    // FIXME: consider size_t or other more constrained types
    int case_id;
    int entry_id;
    int object_id;
    int table_size;
    uint64_t opt_num;
    double load_factor;
    double hit_percent;
    int quotienting_tail_length;
    int bin_size;
    std::string path;
};
}  // namespace tinyptr