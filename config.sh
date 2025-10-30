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

ycsb_del_c_exe_file="$ycsb_workloads_dir/txnsc_del_unif_int.dat"

# Define CPU ID array, ordered by logical core id preference
cpu_id_array=(0 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 23 24 25 26 27 28 29 30 31)

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