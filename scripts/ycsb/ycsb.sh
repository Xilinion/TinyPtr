#!/bin/bash

cd "$(dirname "$0")"

source ../../config.sh

mkdir -p "$index_microbench_dir"
git clone https://github.com/wangziqi2016/index-microbench.git "$index_microbench_dir"
cd "$index_microbench_dir"
git checkout 74cafa57d74798f209d8fcbce8c4f317ce066eae

curl -O --location https://github.com/brianfrankcooper/YCSB/releases/download/0.11.0/ycsb-0.11.0.tar.gz
tar xfvz ycsb-0.11.0.tar.gz
mv ycsb-0.11.0 YCSB

cd -

# move the specs
cp generate_all_workloads.sh "$index_microbench_dir/"
cp workloada "$index_microbench_dir/workload_spec/"
cp workloadb "$index_microbench_dir/workload_spec/"
cp workloadc "$index_microbench_dir/workload_spec/"

cd -

export PYTHON=python2
sed -i '1s|^.*$|#!/usr/bin/env python2|' "$index_microbench_dir/YCSB/bin/ycsb"
mkdir -p "$index_microbench_dir/workloads"
sudo make generate_workload
unset PYTHON

cd -

bash ycsb_neg.sh