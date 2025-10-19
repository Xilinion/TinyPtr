#!/bin/bash

# Source the config file to get shared variables
source ./config.sh

# Use exp_dir from config.sh
echo "Experiment Directory: $exp_dir"

# read arguments
helpFunction() {
    echo ""
    echo "Usage: $0 -d exp_dir"
    echo -e "\t-d the experiment results directory"
    exit 1 # Exit script after printing help
}

while getopts "d:" opt; do
    case "$opt" in
    d) exp_dir="$OPTARG" ;;
    ?) helpFunction ;; # Print helpFunction in case parameter is non-existent
    esac
done

# Print helpFunction in case parameters are empty
if [ -z "$exp_dir" ]; then
    echo "Some or all of the parameters are empty"
    helpFunction
fi

# Begin script in case all parameters are correct
exp_dir="$(realpath $exp_dir)"
echo "$exp_dir"

mkdir -p "$exp_dir/results/"
mkdir -p "$exp_dir/results/figure"

cd ./scripts || exit
bash benchmark.sh -d "$exp_dir"

cd -

# exit

cd ./scripts || exit
bash to_csv.sh -d "$exp_dir"

cd -