#!/bin/bash

# Source the config file to get shared variables
source ../config.sh


while getopts "d:" opt; do
    case "$opt" in
    d)
        exp_dir="$OPTARG"
        ;;
    ?) helpFunction ;; # Print helpFunction in case parameter is non-existent
    esac
done

exp_dir="$(realpath $exp_dir)"

# Use the results directory from config
results_dir="$exp_dir/results"

echo "Using results directory: $results_dir"

# Generate Python config file that all scripts can import
cat > config_py.py << EOF
# Auto-generated configuration file
# Generated from config.sh by to_csv.sh

import os

# Results directory from config.sh
RESULTS_DIR = "$results_dir"
CSV_OUTPUT_DIR = os.path.join(RESULTS_DIR, "csv")

# Other config variables if needed
EXP_DIR = "$exp_dir"
INDEX_MICROBENCH_DIR = "$index_microbench_dir"

print(f"Using results directory: {RESULTS_DIR}")
EOF

echo "Generated config_py.py with results directory: $results_dir"

# Run all CSV generation scripts (they will import config_py.py)
sudo python3 ./ycsb_to_csv.py
sudo python3 ./micro_to_csv.py
sudo python3 ./scaling_to_csv.py
sudo python3 ./progressive_latency_to_csv.py
sudo python3 ./load_factor_support_to_csv.py
sudo python3 ./throughput_space_eff_to_csv.py
sudo python3 ./tagging_with_type.py