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
    rm -rf ./build
    cmake -B build -DCMAKE_BUILD_TYPE=Debug -Wno-dev | tail -n 90
    # cmake --build build -j8 | tail -n 90
    cmake --build build --config Debug -j8 | tail -n 90
    # cmake --build build --config Release -j8 | tail -n 90
    cd scripts

    sudo setcap CAP_SYS_RAWIO+eip ../build/tinyptr
}

function Run() {
    #####native execution
    echo "== benchmark with perf: -o $object_id -c $case_id -e $entry_id -t $table_size -p  $opt_num -l $load_factor -h $hit_percent -b $bin_size -q $quotient_tail_length -f "$res_path" =="

    perf stat -e task-clock,context-switches,cpu-migrations,page-faults,cycles,stalled-cycles-frontend,stalled-cycles-backend,instructions,branches,branch-misses,L1-dcache-loads,L1-dcache-load-misses,L1-dcache-prefetches,L1-icache-loads,L1-icache-load-misses,branch-load-misses,branch-loads,LLC-loads,LLC-load-misses,dTLB-loads,dTLB-load-misses,cache-misses,cache-references -o "$res_path/object_${object_id}_case_${case_id}_entry_${entry_id}_perf.txt" ../build/tinyptr -o $object_id -c $case_id -e $entry_id -t $table_size -p $opt_num -l $load_factor -h $hit_percent -b $bin_size -q $quotient_tail_length -f "$res_path"

    ../build/tinyptr -o $object_id -c $case_id -e $entry_id -t $table_size -p $opt_num -l $load_factor -h $hit_percent -b $bin_size -q $quotient_tail_length -f "$res_path"

    echo "== file path: "$res_path/object_${object_id}_case_${case_id}_entry_${entry_id}_.txt""
}

function FlameGraph() {
    #####native execution
    echo "== benchmark with perf: -o $object_id -c $case_id -e $entry_id -t $table_size -p  $opt_num -l $load_factor -h $hit_percent -b $bin_size -q $quotient_tail_length -f "$res_path" =="

    perf record -F 499 -a -g -- ../build/tinyptr -o $object_id -c $case_id -e $entry_id -t $table_size -p $opt_num -l $load_factor -h $hit_percent -b $bin_size -q $quotient_tail_length -f "$res_path"

    perf script >"$res_path/out.perf"

    ../build/stackcollapse-perf.pl "$res_path/out.perf" >"$res_path/out.folded"

    ../build/flamegraph.pl "$res_path/out.folded" >"$res_path/${object_id}_${case_id}_${entry_id}_kernel.svg"
}

compile

# exit

table_size=1
opt_num=0
load_factor=0
hit_percent=0
quotient_tail_length=16
bin_size=127

for case_id in 12 13; do
    for object_id in 0; do
        entry_id=0
        for opt_num in 10000 100000 1000000 10000000 100000000; do
            Run
            let "entry_id++"
        done
    done
done

for case_id in 0; do
    # for object_id in 4; do
    for object_id in 0 4; do
        entry_id=0
        # for table_size in 100000 ; do
        for table_size in 10000 100000 1000000 10000000; do
            Run
            let "entry_id++"
        done
    done
done

for case_id in 0; do
    for object_id in 4; do
        entry_id=10
        for table_size in 10000 100000 1000000 10000000; do
            for bin_size in 127 63 31 15; do
                for quotient_tail_length in {16..28}; do
                    Run
                    let "entry_id++"
                done
            done
        done
    done
done

for case_id in $(seq 1 7); do
    for object_id in 4; do
        entry_id=1000
        for table_size in 100000 1000000 10000000 100000000; do
            for opt_num in 100000 1000000 10000000 100000000; do
                for bin_size in 127 63 31 15; do
                    for quotient_tail_length in {16..28}; do
                        Run
                        let "entry_id++"
                    done
                done
            done
        done
    done
done

quotient_tail_length=16
bin_size=127

for case_id in $(seq 1 7); do
    for object_id in 0 2 3 4; do
        entry_id=0
        for table_size in 10000 100000 1000000 10000000; do
            for opt_num in 10000 100000 1000000 10000000; do
                Run
                let "entry_id++"
            done
        done
    done
done

for case_id in 8; do
    for object_id in $(seq 0 4); do
        entry_id=0
        for table_size in 10000000; do
            for opt_num in 100000000; do
                for hit_percent in 0.1 0.4 0.8; do
                    for rep_cnt in $(seq 0 0); do
                        Run
                        let "entry_id++"
                    done
                done
            done
        done
    done
done

for case_id in 9 10; do
    for object_id in $(seq 0 4); do
        entry_id=0
        for table_size in 10000 100000 1000000 10000000; do
            for opt_num in 100000 1000000 10000000 100000000; do
                for load_factor in 0.1 0.97; do
                    for rep_cnt in $(seq 0 0); do
                        Run
                        let "entry_id++"
                    done
                done
            done
        done
    done
done

for case_id in 11; do
    for object_id in $(seq 0 4); do
        entry_id=0
        for table_size in 10000 100000 1000000 10000000; do
            for opt_num in 100000 1000000 10000000 100000000; do
                for load_factor in 0.1 0.97; do
                    for hit_percent in 0.1 0.8; do
                        for rep_cnt in $(seq 0 0); do
                            Run
                            let "entry_id++"
                        done
                    done
                done
            done
        done
    done
done
