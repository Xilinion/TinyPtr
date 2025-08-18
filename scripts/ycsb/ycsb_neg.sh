#!/bin/bash

# Source the config file to get shared variables
source ../../config.sh

python3 ./ycsb_neg.py $ycsb_a_load_file $ycsb_a_exe_file $ycsb_neg_a_exe_file
python3 ./ycsb_neg.py $ycsb_b_load_file $ycsb_b_exe_file $ycsb_neg_b_exe_file
python3 ./ycsb_neg.py $ycsb_c_load_file $ycsb_c_exe_file $ycsb_neg_c_exe_file