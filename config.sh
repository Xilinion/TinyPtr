# Define shared variables
exp_dir="."

# Define base directory for workloads
ycsb_workloads_dir="$HOME/tmp/index-microbench/workloads"

# Define new variables using the base directory
ycsb_a_load_file="$ycsb_workloads_dir/loada_unif_int.dat"
ycsb_a_exe_file="$ycsb_workloads_dir/txnsa_unif_int.dat"
ycsb_b_load_file="$ycsb_workloads_dir/loadb_unif_int.dat"
ycsb_b_exe_file="$ycsb_workloads_dir/txnsb_unif_int.dat"
ycsb_c_load_file="$ycsb_workloads_dir/loadc_unif_int.dat"
ycsb_c_exe_file="$ycsb_workloads_dir/txnsc_unif_int.dat"

ycsb_neg_a_exe_file="$ycsb_workloads_dir/txnsa_neg_unif_int.dat"
ycsb_neg_b_exe_file="$ycsb_workloads_dir/txnsb_neg_unif_int.dat"
ycsc_neg_c_exe_file="$ycsb_workloads_dir/txnsc_neg_unif_int.dat"