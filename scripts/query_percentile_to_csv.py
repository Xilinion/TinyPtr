import os
import re
import csv
import config_py  # Import the shared configuration


def extract_percentile_data(file_path):
    """Extract percentile latency data from a result file."""
    with open(file_path, 'r') as f:
        content = f.read()

        # Find the percentile data section
        percentile_section = re.search(
            r'Percentile Latency Records:(.*?)(?=\n\n|\Z)', 
            content, re.DOTALL
        )
        
        if not percentile_section:
            return None

        # Extract all percentile data lines
        data_lines = []
        for line in percentile_section.group(1).strip().split('\n'):
            line = line.strip()
            if line and ',' in line and not line.startswith('Operation Type'):
                # Parse: operation_type, percentile, latency
                parts = [part.strip() for part in line.split(',')]
                if len(parts) == 3:
                    try:
                        operation_type = int(parts[0])
                        percentile = float(parts[1])
                        latency = int(parts[2])
                        data_lines.append((operation_type, percentile, latency))
                    except ValueError:
                        continue

        return data_lines


def extract_performance_metrics(file_path):
    """Extract insert and query performance metrics from a result file."""
    with open(file_path, 'r') as f:
        content = f.read()
        
        metrics = {}
        
        # Extract insert metrics
        insert_cpu_match = re.search(r'Insert CPU Time: (\d+) ms', content)
        insert_throughput_match = re.search(r'Insert Throughput: (\d+) ops/s', content)
        insert_latency_match = re.search(r'Insert Latency: (\d+) ns/op', content)
        
        if insert_cpu_match:
            metrics['insert_cpu_time_ms'] = int(insert_cpu_match.group(1))
        if insert_throughput_match:
            metrics['insert_throughput_ops_per_sec'] = int(insert_throughput_match.group(1))
        if insert_latency_match:
            metrics['insert_latency_ns_per_op'] = int(insert_latency_match.group(1))
        
        # Extract query metrics
        query_cpu_match = re.search(r'Query CPU Time: (\d+) ms', content)
        query_throughput_match = re.search(r'Query Throughput: (\d+) ops/s', content)
        query_latency_match = re.search(r'Query Latency: (\d+) ns/op', content)
        
        if query_cpu_match:
            metrics['query_cpu_time_ms'] = int(query_cpu_match.group(1))
        if query_throughput_match:
            metrics['query_throughput_ops_per_sec'] = int(query_throughput_match.group(1))
        if query_latency_match:
            metrics['query_latency_ns_per_op'] = int(query_latency_match.group(1))
        
        return metrics


def get_load_factor_from_entry_id(entry_id):
    """Map entry_id to load_factor for case 26."""
    load_factor_map = {
        0: 0.7,
        1: 0.8,
        2: 0.9,
        3: 0.95
    }
    return load_factor_map.get(entry_id, None)


def main():
    # Use the shared configuration
    base_dir = config_py.RESULTS_DIR
    output_dir = config_py.CSV_OUTPUT_DIR

    print(f"Processing query percentile results from {base_dir}")
    print(f"Output will be saved to {output_dir}")

    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)

    # List to store all data entries
    all_data = []

    # Define valid IDs for query percentile case (26)
    case_id = 26  # QUERY_HIT_CUSTOM_LOAD_FACTOR_ONLY_PERCENTILE
    valid_object_ids = [6, 7, 15, 17, 20, 24]  # no_resize_object_ids from benchmark.sh
    valid_entry_ids = [0, 1, 2, 3]  # Corresponding to load factors 0.7, 0.8, 0.9, 0.95

    # Process only the valid result files
    for object_id in valid_object_ids:
        for entry_id in valid_entry_ids:
            filename = f"object_{object_id}_case_{case_id}_entry_{entry_id}_.txt"
            file_path = os.path.join(base_dir, filename)

            if os.path.exists(file_path):
                # Get load factor for this entry
                load_factor = get_load_factor_from_entry_id(entry_id)
                if load_factor is None:
                    print(f"Warning: Unknown entry_id {entry_id} for case {case_id}")
                    continue

                # Read percentile data
                percentile_data = extract_percentile_data(file_path)

                # Read performance metrics
                performance_metrics = extract_performance_metrics(file_path)

                # Skip if we couldn't extract the percentile data
                if percentile_data is None:
                    print(f"Warning: Could not extract percentile data from {filename}")
                    continue

                # Store the data
                for operation_type, percentile, latency in percentile_data:
                    entry = {
                        'case_id': case_id,
                        'entry_id': entry_id,
                        'object_id': object_id,
                        'load_factor': load_factor,
                        'operation_type': operation_type,
                        'percentile': percentile,
                        'latency_ns': latency
                    }
                    
                    # Add performance metrics to the entry
                    entry.update(performance_metrics)
                    
                    all_data.append(entry)

                print(f"Processed {filename} (load_factor={load_factor})")
            else:
                print(f"File not found: {filename}")

    # Write a single CSV file
    csv_filename = "query_percentile_results.csv"
    csv_path = os.path.join(output_dir, csv_filename)

    # Define fieldnames including all possible performance metrics
    fieldnames = [
        'case_id', 'entry_id', 'object_id', 'load_factor', 
        'operation_type', 'percentile', 'latency_ns',
        'insert_cpu_time_ms', 'insert_throughput_ops_per_sec', 'insert_latency_ns_per_op',
        'query_cpu_time_ms', 'query_throughput_ops_per_sec', 'query_latency_ns_per_op'
    ]

    with open(csv_path, 'w', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        
        # Write header
        writer.writeheader()
        
        # Write data
        for entry in all_data:
            writer.writerow(entry)

    print(f"Created {csv_path}")
    print(f"Total records: {len(all_data)}")
    
    # Print summary statistics
    if all_data:
        load_factors = sorted(set(entry['load_factor'] for entry in all_data))
        object_ids = sorted(set(entry['object_id'] for entry in all_data))
        print(f"Load factors processed: {load_factors}")
        print(f"Object IDs processed: {object_ids}")


if __name__ == "__main__":
    main()
