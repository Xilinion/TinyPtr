#pragma warning "Ignore '#pragma once' in main file"

#include <unistd.h>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <map>
#include <string>
#include <utility>
#include "dereference_table_64.h"
#include "xxhash64.h"


void configuring_getopt() {
    opterr = 1;
}

int main(int argc, char** argv) {

    bool ifrand(0), iffile(0);
    std::string input_file;
    configuring_getopt();
    for (int c; (c = getopt(argc, argv, "rf:")) != -1;) {
        switch (c) {
            case 'r':
                ifrand = 1;
                break;
            case 'f':
                input_file = optarg;
                break;
            case '?':
                if (optopt == 'f')
                    fprintf(stderr, "Option -%c requires an argument.\n",
                            optopt);
                else if (isprint(optopt))
                    fprintf(stderr, "Unknown option `-%c'.\n", optopt);
                else
                    fprintf(stderr, "Unknown option character `\\x%x'.\n",
                            optopt);
            default:
                abort();
        }
    }
    return 0;
}