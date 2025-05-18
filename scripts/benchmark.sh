#!/bin/bash

# Source the config file to get shared variables
source ../config.sh

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

function Init() {
    compile_option=1
    sudo sysctl kernel.perf_event_paranoid=-1
    sudo sysctl kernel.kptr_restrict=0
    sudo sysctl kernel.yama.ptrace_scope=0
    echo "always" | sudo tee /sys/kernel/mm/transparent_hugepage/enabled
    sudo chmod 777 ../scripts/FlameGraph/stackcollapse-perf.pl
    sudo chmod 777 ../scripts/FlameGraph/flamegraph.pl
}

function Compile() {
    cd ..
    # rm -rf ./build
    cmake -B build -DCMAKE_BUILD_TYPE=Release -Wno-dev | tail -n 90
    cmake --build build --config Release -j8 | tail -n 90
    cd scripts

    sudo setcap CAP_SYS_RAWIO+eip ../build/tinyptr
}

function DebugCompile() {
    cd ..
    # rm -rf ./build
    cmake -B build -DCMAKE_BUILD_TYPE=Debug | tail -n 90
    cmake --build build --config Debug -j8 | tail -n 90
    cd scripts

    sudo setcap CAP_SYS_RAWIO+eip ../build/tinyptr
}

function ValgrindCompile() {
    cd ..
    rm -rf ./build
    cmake -B build -DCMAKE_BUILD_TYPE=Debug -DCOMPILE_FOR_VALGRIND=ON -DCOMPILE_FOR_VALGRIND=ON | tail -n 90
    cmake --build build --config Debug -j8 | tail -n 90
    cd scripts

    sudo setcap -r ../build/tinyptr
}

function AsanCompile() {
    cd ..
    rm -rf ./build
    cmake -B build -DCMAKE_BUILD_TYPE=Debug -DCOMPILE_FOR_ASAN=ON | tail -n 90
    cmake --build build --config Debug -j8 | tail -n 90
    cd scripts

    sudo setcap CAP_SYS_RAWIO+eip ../build/tinyptr
}

function CompileWithOption() {
    if [ $compile_option -eq 1 ]; then
        Compile
    elif [ $compile_option -eq 2 ]; then
        DebugCompile
    elif [ $compile_option -eq 3 ]; then
        ValgrindCompile
    elif [ $compile_option -eq 4 ]; then
        AsanCompile
    fi
}

function CommonArgs() {
    echo "-o $object_id -c $case_id -e $entry_id -t $table_size -p $opt_num -l $load_factor -h $hit_percent -b $bin_size -q $quotient_tail_length -n $thread_num -z $zipfian_skew -f "$res_path""
}

function WarmUp() {
    echo "== begin warming up: $args"
    numactl --cpunodebind=1 --membind=1 ../build/tinyptr $args
}

function Run() {
    #####native execution

    args=$(CommonArgs)

    WarmUp

    echo "== begin benchmarking: $args"

    output=$(numactl --cpunodebind=1 --membind=1 ../build/tinyptr $args 2>&1)

    echo "$output"

    echo "== end benchmarking: $args"

    echo "== file path: "$res_path/object_${object_id}_case_${case_id}_entry_${entry_id}_.txt""
}

function RunYCSB() {

    if [ $case_id -eq 17 ]; then
        ycsb_load_path=$ycsb_a_load_file
        ycsb_exe_path=$ycsb_a_exe_file
    elif [ $case_id -eq 18 ]; then
        ycsb_load_path=$ycsb_b_load_file
        ycsb_exe_path=$ycsb_b_exe_file
    elif [ $case_id -eq 19 ]; then
        ycsb_load_path=$ycsb_c_load_file
        ycsb_exe_path=$ycsb_c_exe_file
    fi

    args="-o $object_id -c $case_id -e $entry_id -t $table_size -p $opt_num -l $load_factor -h $hit_percent -b $bin_size -q $quotient_tail_length -y $ycsb_load_path -s $ycsb_exe_path -n $thread_num -f "$res_path""

    WarmUp

    echo "== begin benchmarking: $args"

    output=$(numactl --cpunodebind=1 --membind=1 ../build/tinyptr $args 2>&1)

    echo "$output"

    echo "== end benchmarking: $args"

    echo "== file path: "$res_path/object_${object_id}_case_${case_id}_entry_${entry_id}_.txt""
}

function RunRandMemFree() {
    #####native execution

    args="-o $object_id -c $case_id -e $entry_id -t $table_size -p $opt_num -l $load_factor -h $hit_percent -b $bin_size -q $quotient_tail_length -n $thread_num -m -f "$res_path""

    WarmUp

    echo "== begin benchmarking: $args"

    numactl --cpunodebind=1 --membind=1 ../build/tinyptr $args &

    # sleep 1

    pid=$!

    echo "pid: $pid"

    log_file="$res_path/object_${object_id}_case_${case_id}_entry_${entry_id}_memuse.txt"

    psrecord $pid --interval 0.1 --log $log_file --plot plot.png

    wait $pid

    echo "== end benchmarking: $args"

    echo "== file path: "$res_path/object_${object_id}_case_${case_id}_entry_${entry_id}_memuse.txt""
}

function RunPerf() {

    args=$(CommonArgs)

    perf stat -a -e task-clock,context-switches,cpu-migrations,page-faults,cycles,stalled-cycles-frontend,stalled-cycles-backend,instructions,branches,branch-misses,L1-dcache-loads,L1-dcache-load-misses,L1-dcache-prefetches,L1-icache-loads,L1-icache-load-misses,branch-load-misses,branch-loads,LLC-loads,LLC-load-misses,dTLB-loads,dTLB-load-misses,cache-misses,cache-references -o "$res_path/object_${object_id}_case_${case_id}_entry_${entry_id}_perf.txt" -- ../build/tinyptr $args

    echo "== benchmark with perf: $args"

    echo "== file path: "$res_path/object_${object_id}_case_${case_id}_entry_${entry_id}_perf.txt""
}

function FlameGraph() {

    args=$(CommonArgs)

    perf record -F 499 -a -g -- ../build/tinyptr $args

    perf script >"$res_path/out.perf"

    ../scripts/FlameGraph/stackcollapse-perf.pl "$res_path/out.perf" >"$res_path/out.folded"

    ../scripts/FlameGraph/flamegraph.pl "$res_path/out.folded" >"$res_path/${object_id}_${case_id}_${entry_id}_kernel.svg"

    echo "== benchmark with flamegraph: $args"

    echo "== file path: "$res_path/${object_id}_${case_id}_${entry_id}_kernel.svg""
}

function FlameGraphEntry() {
    # warm up
    args=$(CommonArgs)

    echo "== benchmark with flamegraph: $args"

    perf record -F 499 --call-graph dwarf -e $flamegraph_entry -a -g -- ../build/tinyptr $args

    perf script -F comm,pid,tid,cpu,time,event,ip,sym,dso >"$res_path/out.perf"

    ../scripts/FlameGraph/stackcollapse-perf.pl "$res_path/out.perf" >"$res_path/out.folded"

    ../scripts/FlameGraph/flamegraph.pl "$res_path/out.folded" >"$res_path/${object_id}_${case_id}_${entry_id}_${flamegraph_entry}.svg"

    echo "== file path: "$res_path/${object_id}_${case_id}_${entry_id}_${flamegraph_entry}.svg""
}

function RunValgrind() {
    args=$(CommonArgs)

    echo "== benchmark with valgrind: $args"

    cachegrind_prefix="$res_path/object_${object_id}_case_${case_id}_entry_${entry_id}"

    sudo valgrind --tool=cachegrind --cachegrind-out-file="${cachegrind_prefix}_cachegrind.out.txt" ../build/tinyptr $args >"${cachegrind_prefix}_cachegrind_stdout.txt"

    sudo cg_annotate --show=Dr,DLmr,Dw,DLmw "${cachegrind_prefix}_cachegrind.out.txt" --auto=yes >"${cachegrind_prefix}_cachegrind_annotate.txt"

    echo "== file path: "${cachegrind_prefix}_cachegrind_stdout.txt""
    echo "== file path: "${cachegrind_prefix}_cachegrind.out.txt""
    echo "== file path: "${cachegrind_prefix}_cachegrind_annotate.txt""
}

function RunValgrindCheckLeak() {
    args=$(CommonArgs)

    echo "== benchmark with valgrind: $args"

    sudo valgrind --tool=memcheck --leak-check=full --show-reachable=yes --track-origins=yes ../build/tinyptr $args
}

function RunCTest() {
    ctest --test-dir ../build
}

Init

# Compile
# DebugCompile
# ValgrindCompile

compile_option=1
CompileWithOption

thread_num=0
zipfian_skew=0

table_size=1
opt_num=0
load_factor=1
hit_percent=0
quotient_tail_length=0
bin_size=127

# RunCTest

# ../build/junction_test
# exit

# ../build/blast_test
# exit

# ../build/concurrent_skulker_ht_test

for case_id in 1 6 7; do
    for object_id in 20; do
        entry_id=0
        for table_size in 16777215; do
            opt_num=$table_size
            Run
            let "entry_id++"
        done
    done
done

exit

# thread_num=16
# for case_id in 17 18 19; do
#     for object_id in 14; do
#         entry_id=10000
#         for table_size in 268435456; do
#         # for table_size in 161061273; do
#         # for table_size in 67108864; do
#             # for asdf in 1 2; do
#             output=$(RunYCSB)
#             echo "$output"
#             let "entry_id++"
#             # done
#         done
#     done
# done
# thread_num=0

# exit

# thread_num=16
# for case_id in 1 6 7; do
#     for object_id in 14; do
#         entry_id=10000
#         for table_size in 67108864; do
#             opt_num=63753420
#             output=$(Run)
#             echo "$output"
#             let "entry_id++"
#         done
#     done
# done
# thread_num=0

# exit

# thread_num=1
# for case_id in 9 10; do
#     for object_id in 14; do
#         entry_id=10000
#         for table_size in 16777216; do
#             # for load_factor in 0.5; do
#             for load_factor in 0.5; do
#                 opt_num=$table_size
#                 output=$(Run)
#                 echo "$output"
#                 let "entry_id++"
#             done
#         done
#     done
# done
# thread_num=0

# exit

# for case_id in 7; do
#     for object_id in 19; do
#         entry_id=10000
#         # for table_size in 16777215; do
#         for table_size in 8000000; do
#             opt_num=$table_size
#             RunValgrind
#             let "entry_id++"
#         done
#     done
# done

# exit

for case_id in 1; do
    for object_id in 4 12; do
        entry_id=100
        for table_size in 16777215; do
            for load_factor in 0.05 0.1 0.15 0.2 0.25 0.3 0.35 0.4 0.45 0.5 0.55 0.6 0.65 0.7 0.75 0.8 0.85 0.9 0.95; do
                opt_num=$(printf "%.0f" $(echo "$table_size * $load_factor" | bc -l))
                output=$(Run)
                echo "$output"
                let "entry_id++"
            done
        done
    done
done

for case_id in 9 10; do
    for object_id in 4 12; do
        entry_id=0
        for table_size in 16777215; do
            for load_factor in 0.1 0.2 0.3 0.4 0.5 0.6 0.7 0.8 0.9 0.95; do
                opt_num=$table_size
                output=$(Run)
                echo "$output"
                let "entry_id++"
            done
        done
    done
done

exit

# for case_id in 1; do
#     for object_id in 7; do
#         entry_id=0
#         for table_size in 16777216 67108864; do
#             opt_num=134217728
#             Run
#             let "entry_id++"
#         done
#     done
# done

# thread_num=0
# for case_id in 6 7; do
#     for object_id in 12; do
#         # for object_id in 13; do
#         entry_id=0
#         for table_size in 17108864; do
#             opt_num=15753420
#             Run
#             # RunRandMemFree
#             # output=$(Run)
#             # echo "$output"
#             let "entry_id++"
#         done
#     done
# done
# thread_num=0

# exit

# thread_num=16
# for case_id in 17 18 19; do
#     for object_id in 16 15; do
#         entry_id=1000
#         for table_size in 268435456; do
#         # for table_size in 161061273; do
#         # for table_size in 67108864; do
#             # for asdf in 1 2; do
#             output=$(RunYCSB)
#             echo "$output"
#             let "entry_id++"
#             # done
#         done
#     done
# done
# thread_num=0

# exit

# YCSB without resize

thread_num=16
for case_id in 17 18 19; do
    for object_id in 6 7 14 15 17; do
        entry_id=0
        for table_size in 268435456; do
            output=$(RunYCSB)
            echo "$output"
            let "entry_id++"
        done
    done
done
thread_num=0

# YCSB with resize

thread_num=16
for case_id in 17 18 19; do
    for object_id in 6 7 16 15 18; do
        entry_id=1
        for table_size in 16777216; do
            output=$(RunYCSB)
            echo "$output"
            let "entry_id++"
        done
    done
done
thread_num=0

# micro benchmark

thread_num=16
for case_id in 1 3 6 7; do
    for object_id in 6 7 14 15 17; do
        entry_id=0
        for table_size in 67108864; do
            opt_num=63753420
            output=$(Run)
            echo "$output"
            let "entry_id++"
        done
    done
done
thread_num=0

# scaling

for case_id in 1 3 6 7; do
    for object_id in 6 7 14 15 17; do
        entry_id=10
        for table_size in 67108864; do
            opt_num=63753420
            for thread_num in 1 2 4 8 16 32; do
                output=$(Run)
                echo "$output"
                let "entry_id++"
            done
        done
    done
done
thread_num=0

# progressive latency

for case_id in 9 10; do
    for object_id in 4 6 7 12 15; do
        entry_id=0
        for table_size in 16777216; do
            for load_factor in 0.1 0.2 0.3 0.4 0.5 0.6 0.7 0.8 0.9 0.95; do
                opt_num=$table_size
                output=$(Run)
                echo "$output"
                let "entry_id++"
            done
        done
    done
done

# load factor support

for case_id in 0; do
    for object_id in 4; do
        entry_id=0
        for table_size in 67108864; do
            for bin_size in 3 7 15 31 63 127; do
                opt_num=$table_size
                output=$(Run)
                echo "$output"
                let "entry_id++"
            done
        done
    done
done

# Throughput / Space Efficiency

for case_id in 1; do
    for object_id in 4 6 7 12 15; do
        entry_id=100
        for table_size in 16777216; do
            for load_factor in 0.05 0.1 0.15 0.2 0.25 0.3 0.35 0.4 0.45 0.5 0.55 0.6 0.65 0.7 0.75 0.8 0.85 0.9 0.95; do
                opt_num=$(printf "%.0f" $(echo "$table_size * $load_factor" | bc -l))
                output=$(RunRandMemFree)
                echo "$output"
                output=$(Run)
                echo "$output"
                let "entry_id++"
            done
        done
    done
done

exit

# load factor with deletion

for case_id in 16; do
    for object_id in 4; do
        entry_id=0
        for table_size in 67108864; do
            opt_num=$table_size
            for bin_size in 3 7 15 31 63 127; do
                for load_factor in 0.99 0.98 0.97 0.96 0.95 0.94 0.93 0.92 0.91 0.90 0.89 0.88 0.87 0.86 0.85 0.84 0.83 0.82 0.81 0.80 0.79 0.78 0.77 0.76 0.75 0.74 0.73 0.72 0.71 0.70 0.69 0.68 0.67 0.66 0.65 0.64 0.63 0.62 0.61 0.60 0.59 0.58 0.57 0.56 0.55 0.54 0.53 0.52 0.51 0.50 0.49 0.48 0.47 0.46 0.; do
                    output=$(Run)
                    echo "$output"

                    let "entry_id++"
                    if echo "$output" | grep -q "good"; then
                        break
                    fi
                done
            done

        done
    done
done

exit

for case_id in 0; do
    for object_id in 4; do
        # for object_id in 3 4 6 7 10 12; do
        entry_id=0
        # for table_size in 1000000 2000000 4000000 8000000 16000000 32000000 64000000 128000000; do
        for table_size in 270000000; do
            for bin_size in 3 7 15 31 63 127; do
                opt_num=$table_size

                # RunValgrind
                # RunPerf
                # FlameGraph
                # RunRandMemFree
                # sleep 3
                output=$(Run)
                echo "$output"

                let "entry_id++"
            done
        done
    done
done

for case_id in 16; do
    for object_id in 8; do
        entry_id=0
        for table_size in 10000000; do
            opt_num=$table_size
            Run
            let "entry_id++"
        done
    done
done

exit

for case_id in 15; do
    for object_id in 4; do
        entry_id=0
        for table_size in 1000000 10000000; do
            opt_num=$table_size
            Run
            let "entry_id++"
        done
    done
done

exit

for case_id in $(seq 6 7); do
    if [ $case_id -eq 5 ]; then
        continue
    fi
    for object_id in 4 7; do
        entry_id=0
        for table_size in 10000000; do
            opt_num=$table_size

            for flamegraph_entry in cache-misses branch-misses LLC-loads-misses page-faults L1-dcache-load-misses L1-icache-load-misses; do
                FlameGraphEntry
            done

            let "entry_id++"
        done
    done
done

# exit

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

quotient_tail_length=0
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
