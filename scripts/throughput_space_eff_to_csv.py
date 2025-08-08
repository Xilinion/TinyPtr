import os
import re
import csv
import config_py  # Import the shared configuration


def extract_throughput(file_path):
    """Extract Throughput and Latency from a result file."""
    with open(file_path, 'r') as f:
        content = f.read()

        throughput_match = re.search(
            r'Throughput: (\d+) ops/s', content)
        latency_match = re.search(
            r'Latency: (\d+) ns/op', content)

        throughput = int(throughput_match.group(
            1)) if throughput_match else None
        latency = int(latency_match.group(1)
                      ) if latency_match else None

        return throughput, latency


def extract_memory_usage(file_path):
    """Extract max and min real and virtual memory usage from a memory usage file."""
    real_values = []
    virtual_values = []

    with open(file_path, 'r') as f:
        for line in f:
            match = re.match(r'\s*\d+\.\d+\s+\d+\.\d+\s+([\d.]+)\s+([\d.]+)', line)
            if match:
                real = float(match.group(1))
                virtual = float(match.group(2))
                real_values.append(real)
                virtual_values.append(virtual)

    if not real_values or not virtual_values:
        return 0.0, 0.0

    # Calculate max - min for both real and virtual memory
    real_range = max(real_values) - min(real_values)
    virtual_range = max(virtual_values) - min(virtual_values)

    return real_range, virtual_range


def main():
    # Base directory for results
    base_dir = config_py.RESULTS_DIR
    output_dir = config_py.CSV_OUTPUT_DIR

    print(f"Processing results from {base_dir}")
    print(f"Output will be saved to {output_dir}")

    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)

    # Temporarily store all rows before writing
    collected_rows = []

    # Define valid IDs based on the updated bash script
    valid_case_ids = [1, 6, 7]
    valid_object_ids = [6, 7, 15, 17, 20]
    load_factors = [0.05, 0.1, 0.15, 0.2, 0.25, 0.3, 0.35, 0.4, 0.45, 0.5, 0.55, 0.6, 0.65, 0.7, 0.75, 0.8, 0.85, 0.9, 0.95, 0.99]
    num_repetitions = 10

    # Process only the valid result files
    for case_id in valid_case_ids:
        for object_id in valid_object_ids:
            base_entry_id = 100  # Starting entry_id
            for load_factor in load_factors:
                throughputs = []
                latencies = []

                # Per-load-factor memuse is recorded once at base_entry_id
                memuse_filename = f"object_{object_id}_case_{case_id}_entry_{base_entry_id}_memuse.txt"
                memuse_file_path = os.path.join(base_dir, memuse_filename)
                if os.path.exists(memuse_file_path):
                    real_range, virtual_range = extract_memory_usage(memuse_file_path)
                else:
                    real_range, virtual_range = 0.0, 0.0

                # Collect data from all repetitions (result files only)
                for rep in range(num_repetitions):
                    entry_id = base_entry_id + rep
                    filename = f"object_{object_id}_case_{case_id}_entry_{entry_id}_.txt"
                    file_path = os.path.join(base_dir, filename)

                    if os.path.exists(file_path):
                        throughput, latency = extract_throughput(file_path)
                        if throughput is None or latency is None:
                            print(f"Warning: Could not extract throughput/latency from {filename}")
                            continue
                        throughputs.append(throughput)
                        latencies.append(latency)

                # Calculate averages if we have data
                if throughputs:
                    avg_throughput = sum(throughputs) / len(throughputs)
                    avg_latency = sum(latencies) / len(latencies)
                    avg_real_memory = real_range
                    avg_virtual_memory = virtual_range

                    collected_rows.append({
                        'case_id': case_id,
                        'object_id': object_id,
                        'load_factor': load_factor,
                        'avg_throughput (ops/s)': avg_throughput,
                        'avg_latency (ns/op)': avg_latency,
                        'avg_real_memory (MB)': avg_real_memory,
                        'avg_virtual_memory (MB)': avg_virtual_memory,
                    })

                base_entry_id += num_repetitions  # Next load_factor starts at the next entry_id block

    # Build lookup for case 1 real memory by (object_id, load_factor)
    real_mem_case1 = {}
    for row in collected_rows:
        if row['case_id'] == 1:
            key = (row['object_id'], row['load_factor'])
            real_mem_case1[key] = row['avg_real_memory (MB)']

    # Write a single CSV file with additional column: avg_real_memory_case1 (MB)
    csv_filename = "throughput_space_eff_results.csv"
    csv_path = os.path.join(output_dir, csv_filename)

    with open(csv_path, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        # Header
        writer.writerow([
            'case_id', 'object_id', 'load_factor',
            'avg_throughput (ops/s)', 'avg_latency (ns/op)',
            'avg_real_memory (MB)', 'avg_virtual_memory (MB)',
            'avg_real_memory_case1 (MB)'
        ])
        # Rows
        for row in collected_rows:
            key = (row['object_id'], row['load_factor'])
            real_case1 = real_mem_case1.get(key, row['avg_real_memory (MB)'])
            writer.writerow([
                row['case_id'], row['object_id'], row['load_factor'],
                row['avg_throughput (ops/s)'], row['avg_latency (ns/op)'],
                row['avg_real_memory (MB)'], row['avg_virtual_memory (MB)'],
                real_case1
            ])

    print(f"Created {csv_path}")
    print(f"Total data points: {len(collected_rows)}")


if __name__ == "__main__":
    main()
