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

function Init() {
    compile_option=1
    sudo sysctl kernel.perf_event_paranoid=-1
    sudo sysctl kernel.kptr_restrict=0
    sudo sysctl kernel.yama.ptrace_scope=0
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
    rm -rf ./build
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

function Run() {
    #####native execution

    echo "== begin benchmarking: -o $object_id -c $case_id -e $entry_id -t $table_size -p  $opt_num -l $load_factor -h $hit_percent -b $bin_size -q $quotient_tail_length -f "$res_path""

    ../build/tinyptr -o $object_id -c $case_id -e $entry_id -t $table_size -p $opt_num -l $load_factor -h $hit_percent -b $bin_size -q $quotient_tail_length -f "$res_path"

    echo "== end benchmarking: -o $object_id -c $case_id -e $entry_id -t $table_size -p  $opt_num -l $load_factor -h $hit_percent -b $bin_size -q $quotient_tail_length -f "$res_path""

    echo "== file path: "$res_path/object_${object_id}_case_${case_id}_entry_${entry_id}_.txt""
}

function RunPerf() {
    # warm up
    ../build/tinyptr -o $object_id -c $case_id -e $entry_id -t $table_size -p $opt_num -l $load_factor -h $hit_percent -b $bin_size -q $quotient_tail_length -f "$res_path"

    perf stat -a -e task-clock,context-switches,cpu-migrations,page-faults,cycles,stalled-cycles-frontend,stalled-cycles-backend,instructions,branches,branch-misses,L1-dcache-loads,L1-dcache-load-misses,L1-dcache-prefetches,L1-icache-loads,L1-icache-load-misses,branch-load-misses,branch-loads,LLC-loads,LLC-load-misses,dTLB-loads,dTLB-load-misses,cache-misses,cache-references -o "$res_path/object_${object_id}_case_${case_id}_entry_${entry_id}_perf.txt" -- ../build/tinyptr -o $object_id -c $case_id -e $entry_id -t $table_size -p $opt_num -l $load_factor -h $hit_percent -b $bin_size -q $quotient_tail_length -f "$res_path"

    echo "== benchmark with perf: -o $object_id -c $case_id -e $entry_id -t $table_size -p  $opt_num -l $load_factor -h $hit_percent -b $bin_size -q $quotient_tail_length -f "$res_path""

    echo "== file path: "$res_path/object_${object_id}_case_${case_id}_entry_${entry_id}_perf.txt""
}

function FlameGraph() {
    # warm up
    ../build/tinyptr -o $object_id -c $case_id -e $entry_id -t $table_size -p $opt_num -l $load_factor -h $hit_percent -b $bin_size -q $quotient_tail_length -f "$res_path"

    perf record -F 499 -a -g -- ../build/tinyptr -o $object_id -c $case_id -e $entry_id -t $table_size -p $opt_num -l $load_factor -h $hit_percent -b $bin_size -q $quotient_tail_length -f "$res_path"

    perf script >"$res_path/out.perf"

    ../scripts/FlameGraph/stackcollapse-perf.pl "$res_path/out.perf" >"$res_path/out.folded"

    ../scripts/FlameGraph/flamegraph.pl "$res_path/out.folded" >"$res_path/${object_id}_${case_id}_${entry_id}_kernel.svg"

    echo "== benchmark with flamegraph: -o $object_id -c $case_id -e $entry_id -t $table_size -p  $opt_num -l $load_factor -h $hit_percent -b $bin_size -q $quotient_tail_length -f "$res_path""

    echo "== file path: "$res_path/${object_id}_${case_id}_${entry_id}_kernel.svg""
}

function FlameGraphEntry() {
    # warm up
    ../build/tinyptr -o $object_id -c $case_id -e $entry_id -t $table_size -p $opt_num -l $load_factor -h $hit_percent -b $bin_size -q $quotient_tail_length -f "$res_path"

    echo "== benchmark with flamegraph: -o $object_id -c $case_id -e $entry_id -t $table_size -p  $opt_num -l $load_factor -h $hit_percent -b $bin_size -q $quotient_tail_length -f "$res_path""

    perf record -F 499 --call-graph dwarf -e $flamegraph_entry -a -g -- ../build/tinyptr -o $object_id -c $case_id -e $entry_id -t $table_size -p $opt_num -l $load_factor -h $hit_percent -b $bin_size -q $quotient_tail_length -f "$res_path"

    perf script -F comm,pid,tid,cpu,time,event,ip,sym,dso >"$res_path/out.perf"

    ../scripts/FlameGraph/stackcollapse-perf.pl "$res_path/out.perf" >"$res_path/out.folded"

    ../scripts/FlameGraph/flamegraph.pl "$res_path/out.folded" >"$res_path/${object_id}_${case_id}_${entry_id}_${flamegraph_entry}.svg"

    echo "== file path: "$res_path/${object_id}_${case_id}_${entry_id}_${flamegraph_entry}.svg""
}

function RunValgrind() {
    # warm up
    ../build/tinyptr -o $object_id -c $case_id -e $entry_id -t $table_size -p $opt_num -l $load_factor -h $hit_percent -b $bin_size -q $quotient_tail_length -f "$res_path"

    echo "== benchmark with valgrind: -o $object_id -c $case_id -e $entry_id -t $table_size -p  $opt_num -l $load_factor -h $hit_percent -b $bin_size -q $quotient_tail_length -f "$res_path""

    sudo valgrind --tool=cachegrind --cachegrind-out-file="$res_path/object_${object_id}_case_${case_id}_entry_${entry_id}_cachegrind.out.txt" ../build/tinyptr -o $object_id -c $case_id -e $entry_id -t $table_size -p $opt_num -l $load_factor -h $hit_percent -b $bin_size -q $quotient_tail_length -f "$res_path" >"$res_path/object_${object_id}_case_${case_id}_entry_${entry_id}_cachegrind_stdout.txt"

    sudo cg_annotate --show=Dr,DLmr,Dw,DLmw "$res_path/object_${object_id}_case_${case_id}_entry_${entry_id}_cachegrind.out.txt" --auto=yes >"$res_path/object_${object_id}_case_${case_id}_entry_${entry_id}_cachegrind_annotate.txt"

    echo "== file path: "$res_path/object_${object_id}_case_${case_id}_entry_${entry_id}_cachegrind_stdout.txt""
    echo "== file path: "$res_path/object_${object_id}_case_${case_id}_entry_${entry_id}_cachegrind.out.txt""
    echo "== file path: "$res_path/object_${object_id}_case_${case_id}_entry_${entry_id}_cachegrind_annotate.txt""
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

table_size=1
opt_num=0
load_factor=0
hit_percent=0
quotient_tail_length=0
bin_size=127

for case_id in 5; do
    # if [ $case_id -eq 5 ]; then
    #     continue
    # fi
    for object_id in 12; do
        entry_id=0
        for table_size in 10000000; do
            # for table_size in 1000000 10000000; do
            opt_num=$table_size

            # RunValgrind
            # RunPerf
            Run

            let "entry_id++"
        done
    done
done

exit

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
