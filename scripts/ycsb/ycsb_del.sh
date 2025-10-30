#!/bin/bash

# Source the config file to get shared variables
source ../../config.sh

# Generate DELETE workloads based on the load phase of each YCSB workload
python3 ./ycsb_del.py $ycsb_c_load_file $ycsb_del_c_file

