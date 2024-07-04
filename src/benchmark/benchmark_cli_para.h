#pragma once

#include <cstdlib>
#include <string>

class BenchmarkCLIPara {
   private:
    static void configuring_getopt();

   public:
    BenchmarkCLIPara() = default;
    ~BenchmarkCLIPara() = default;

    void Parse(int argc, char** argv);
    std::string GetOuputFileName();

   public:
    int case_id;
    int entry_id;
    int object_id;
};