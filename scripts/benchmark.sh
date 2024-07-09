#!/bin/bash

exp_dir="."

# read arguments
helpFunction() {
    echo ""
    echo "Usage: $0 -d exp_dir"
    echo -e "\t-d the experiment results directory"
    exit 1 # Exit script after printing help
}

while getopts "d:" opt; do
    case "$opt" in
    d)
        exp_dir="$OPTARG"
        res_path=$exp_dir/results
        ;;
    ?) helpFunction ;; # Print helpFunction in case parameter is non-existent
    esac
done

# Print helpFunction in case parameters are empty
if [ -z "$exp_dir" ]; then
    echo "Some or all of the parameters are empty"
    helpFunction
fi

# Begin script in case all parameters are correct
echo "$exp_dir"

function compile() {
    cd ..
    cmake -B build | tail -n 90
    cmake --build build --config Release -j8 | tail -n 90
    cd scripts

    sudo setcap CAP_SYS_RAWIO+eip ../tinyptr
}

function Run() {
    #####native execution
    echo "== benchmark with perf: -o $object_id -c $case_id -e $entry_id -t $table_size -p  $opt_num -l $load_factor -h $hit_percent -f "$res_path" =="

    perf stat -e task-clock,context-switches,cpu-migrations,page-faults,cycles,stalled-cycles-frontend,stalled-cycles-backend,instructions,branches,branch-misses,L1-dcache-loads,L1-dcache-load-misses,L1-dcache-prefetches,L1-icache-loads,L1-icache-load-misses,branch-load-misses,branch-loads,LLC-loads,LLC-load-misses,dTLB-loads,dTLB-load-misses,cache-misses,cache-references -o "$res_path/object_${object_id}_case_${case_id}_entry_${entry_id}_perf.txt" ../build/tinyptr -o $object_id -c $case_id -e $entry_id -t $table_size -p $opt_num -l $load_factor -h $hit_percent -f "$res_path"

    ../build/tinyptr -o $object_id -c $case_id -e $entry_id -t $table_size -p $opt_num -l $load_factor -h $hit_percent -f "$res_path"
}

compile

table_size=0
opt_num=0
load_factor=0
hit_percent=0

# TODO: share enums with scripts

# load factor support for derefence table
entry_id=0
case_id=0
object_id=0
for table_size in 10000 100000 1000000 10000000 100000000; do
    for rep_cnt in $(seq 0 4); do
        Run
        let "entry_id++"
    done
done

entry_id=0
for case_id in $(seq 1 7); do
    for object_id in $(seq 0 4); do
        for table_size in 10000 100000 1000000 10000000 100000000; do
            for opt_num in 10000 100000 1000000 10000000 100000000; do
                for rep_cnt in $(seq 0 4); do
                    Run
                    let "entry_id++"
                done
            done
        done
    done
done

entry_id=0
for case_id in 8; do
    for object_id in $(seq 0 4); do
        for table_size in 10000 100000 1000000 10000000 100000000; do
            for opt_num in 10000 100000 1000000 10000000 100000000; do
                for hit_percent in 0.1 0.2 0.3 0.4 0.5 0.6 0.7 0.8 0.9; do
                    for rep_cnt in $(seq 0 4); do
                        Run
                        let "entry_id++"
                    done
                done
            done
        done
    done
done

entry_id=0
for case_id in 9 10; do
    for object_id in $(seq 0 4); do
        for table_size in 10000 100000 1000000 10000000 100000000; do
            for opt_num in 10000 100000 1000000 10000000 100000000; do
                for load_factor in 0.1 0.2 0.3 0.4 0.5 0.6 0.7 0.8 0.9 0.95 0.97; do
                    for rep_cnt in $(seq 0 4); do
                        Run
                        let "entry_id++"
                    done
                done
            done
        done
    done
done

entry_id=0
for case_id in 11; do
    for object_id in $(seq 0 4); do
        for table_size in 10000 100000 1000000 10000000 100000000; do
            for opt_num in 10000 100000 1000000 10000000 100000000; do
                for load_factor in 0.1 0.2 0.3 0.4 0.5 0.6 0.7 0.8 0.9 0.95 0.97; do
                    for hit_percent in 0.1 0.2 0.3 0.4 0.5 0.6 0.7 0.8 0.9; do
                        for rep_cnt in $(seq 0 4); do
                            Run
                            let "entry_id++"
                        done
                    done
                done
            done
        done
    done
done
