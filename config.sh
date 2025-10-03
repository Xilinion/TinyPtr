# Define shared variables
exp_dir="."
exp_dir="$(realpath $exp_dir)"
echo "exp_dir: $exp_dir"

latex_dir=$exp_dir/latex
echo "latex_dir: $latex_dir"

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

# Define CPU ID array, ordered by logical core id preference
cpu_id_array=(0 16 1 17 2 18 3 19 4 20 5 21 6 22 7 23 8 24 9 25 10 26 11 27 12 28 13 29 14 30 15 31)

# Control whether to bind cores (set to true to enable core binding)
enable_core_binding=false

# Base NUMA command
numactl_base="numactl"
# numactl_base="numactl --cpunodebind=0 --membind=0"

# Function to set numactl_bind based on thread_num and enable_core_binding
declare -g numactl_bind
set_numactl_bind() {
    if [ "$enable_core_binding" = true ] && [ "$thread_num" -gt 0 ]; then
        cpu_list=$(IFS=,; echo "${cpu_id_array[*]:0:$thread_num}")
        numactl_bind="$numactl_base --physcpubind=$cpu_list"
    else
        numactl_bind="$numactl_base"
    fi
}

# Timeout prefix (empty to disable)
timeout_cmd="timeout 300"