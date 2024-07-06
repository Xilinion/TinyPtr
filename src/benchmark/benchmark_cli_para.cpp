
#include "benchmark_cli_para.h"
#include <unistd.h>
#include <cstdio>
#include <string>

namespace tinyptr {

void BenchmarkCLIPara::configuring_getopt() {
    opterr = 1;
}

std::string BenchmarkCLIPara::GetOuputFileName() {

    return std::string("object_") + std::to_string(object_id) +
           std::string("_case_") + std::to_string(case_id) +
           std::string("_entry_") + std::to_string(entry_id) +
           std::string("_.txt");
}

void BenchmarkCLIPara::Parse(int argc, char** argv) {
    this->configuring_getopt();
    for (int c; (c = getopt(argc, argv, "o:c:e:t:p:l:h:f:")) != -1;) {
        switch (c) {
            // TODO: add validity check of parameters
            case 'o':
                object_id = std::stoi(optarg);
                break;
            case 'c':
                case_id = std::stoi(optarg);
                break;
            case 'e':
                entry_id = std::stoi(optarg);
                break;
            case 't':
                table_size = std::stoi(optarg);
                break;
            case 'p':
                opt_num = std::stoull(optarg);
                break;
            case 'l':
                load_factor = std::stod(optarg);
                break;
            case 'h':
                hit_percent = std::stod(optarg);
                break;
            case 'f':
                fprintf(stderr, "Not supported yet.\n");
                break;
            case '?':
                // if (optopt == 'f')
                //     fprintf(stderr, "Option -%c requires an argument.\n",
                //             optopt);
                // else if (isprint(optopt))
                //     fprintf(stderr, "Unknown option `-%c'.\n", optopt);
                // else
                //     fprintf(stderr, "Unknown option character `\\x%x'.\n",
                //             optopt);
                // TODO: support error messages for varying options
                fprintf(stderr,
                        "Too lazy to deal with wrong parameters. Could be an "
                        "unknown option or missing argument.\n");
            default:
                abort();
        }
    }
}
}  // namespace tinyptr