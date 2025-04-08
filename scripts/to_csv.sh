#! /bin/bash

python3 ./ycsb_to_csv.py
python3 ./micro_to_csv.py
python3 ./scaling_to_csv.py
python3 ./progressive_latency_to_csv.py
python3 ./load_factor_support_to_csv.py
python3 ./throughput_space_eff_to_csv.py
python3 ./tagging_with_type.py