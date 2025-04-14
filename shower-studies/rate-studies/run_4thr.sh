#!/bin/bash

# Check if a threshold value is provided as an argument
if [[ -z "$1" ]]; then
    echo "Usage: $0 <threshold>"
    exit 1
fi

# Get the threshold value from the command-line argument
threshold=$1

# Define the base input directory and output options
base_input_dir="/lustrefs/hdd_pool_dir/L1T/Filter/ThresholdScan_Zprime_DY/last/MinBias_PU200/"
output_dir="."
tag_prefix="_thr"

# Search for the input file matching the threshold
input_file=$(find "$base_input_dir" -type f -path "*/0000/*thr${threshold}*.root" | head -n 1)

# Check if the input file exists
if [[ -z "$input_file" ]]; then
    echo "No input file found for threshold $threshold. Exiting..."
    exit 1
fi

# Extract the directory path up to 0000/
input_dir=$(dirname "$input_file")

# Update the threshold value in run_config.yaml
sed -i "s/threshold: [0-9]\+/threshold: $threshold/" run_config.yaml

# Execute the dtpr fill-histos command
dtpr fill-histos -i "$input_dir/" -o "$output_dir" --tag="${tag_prefix}${threshold}"
