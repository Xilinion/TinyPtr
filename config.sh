# Define shared variables
exp_dir="."
exp_dir="$(realpath $exp_dir)"
echo "exp_dir: $exp_dir"

# Define base directory for workloads
tmp_dir="$HOME/tmp"
index_microbench_dir="$HOME/tmp/index-microbench"
ycsb_workloads_dir="$index_microbench_dir/workloads"

# Define new variables using the base directory
ycsb_a_load_file="$ycsb_workloads_dir/loada_unif_int.dat"
ycsb_a_exe_file="$ycsb_workloads_dir/txnsa_unif_int.dat"
ycsb_b_load_file="$ycsb_workloads_dir/loadb_unif_int.dat"
ycsb_b_exe_file="$ycsb_workloads_dir/txnsb_unif_int.dat"
ycsb_c_load_file="$ycsb_workloads_dir/loadc_unif_int.dat"
ycsb_c_exe_file="$ycsb_workloads_dir/txnsc_unif_int.dat"

ycsb_neg_a_exe_file="$ycsb_workloads_dir/txnsa_neg_unif_int.dat"
ycsb_neg_b_exe_file="$ycsb_workloads_dir/txnsb_neg_unif_int.dat"
ycsb_neg_c_exe_file="$ycsb_workloads_dir/txnsc_neg_unif_int.dat"

numactl_bind=""
# numactl_bind="numactl --cpunodebind=0 --membind=0"

# Timeout prefix (empty to disable)
timeout_cmd="timeout 600"