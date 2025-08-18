import os
import re
import csv
import config_py  # Import the shared configuration


def extract_metrics(file_path):
    """Extract Throughput and Latency for both Insert and Query operations."""
    with open(file_path, 'r') as f:
        content = f.read()

        # Extract Insert metrics
        insert_throughput_match = re.search(r'Insert Throughput: (\d+) ops/s', content)
        insert_latency_match = re.search(r'Insert Latency: (\d+) ns/op', content)
        
        # Extract Query metrics
        query_throughput_match = re.search(r'Query Throughput: (\d+) ops/s', content)
        query_latency_match = re.search(r'Query Latency: (\d+) ns/op', content)

        # Get values or None if not found
        insert_throughput = int(insert_throughput_match.group(1)) if insert_throughput_match else None
        insert_latency = int(insert_latency_match.group(1)) if insert_latency_match else None
        query_throughput = int(query_throughput_match.group(1)) if query_throughput_match else None
        query_latency = int(query_latency_match.group(1)) if query_latency_match else None

        return insert_throughput, insert_latency, query_throughput, query_latency


def main():
    # Base directory for results
    base_dir = config_py.RESULTS_DIR
    output_dir = config_py.CSV_OUTPUT_DIR

    print(f"Processing results from {base_dir}")
    print(f"Output will be saved to {output_dir}")

    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)

    # List to store all data entries
    all_data = []

    # Define valid IDs based on the updated bash script
    valid_case_ids = [9, 10]
    valid_object_ids = [6, 7, 15, 17, 20]
    load_factors = [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 0.95]

    # Process only the valid result files
    for case_id in valid_case_ids:
        for object_id in valid_object_ids:
            entry_id = 0  # Starting entry_id
            for load_factor in load_factors:
                filename = f"object_{object_id}_case_{case_id}_entry_{entry_id}_.txt"
                file_path = os.path.join(base_dir, filename)

                if os.path.exists(file_path):
                    # Read metrics data
                    insert_throughput, insert_latency, query_throughput, query_latency = extract_metrics(file_path)

                    # Skip if we couldn't extract the data
                    if any(metric is None for metric in [insert_throughput, insert_latency, query_throughput, query_latency]):
                        print(f"Warning: Could not extract complete metrics from {filename}")
                        continue

                    # Store the data with load_factor
                    all_data.append((
                        case_id, 
                        object_id, 
                        load_factor, 
                        insert_throughput, 
                        insert_latency,
                        query_throughput,
                        query_latency
                    ))

                entry_id += 1  # Increment entry_id for each load_factor

    # Write a single CSV file
    csv_filename = "progressive_latency_results.csv"
    csv_path = os.path.join(output_dir, csv_filename)

    with open(csv_path, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        # Write header
        writer.writerow([
            'case_id', 
            'object_id', 
            'load_factor', 
            'insert_throughput (ops/s)', 
            'insert_latency (ns/op)',
            'query_throughput (ops/s)',
            'query_latency (ns/op)'
        ])
        # Write data
        for entry in all_data:
            writer.writerow(entry)

    print(f"Created {csv_path}")


if __name__ == "__main__":
    main()
