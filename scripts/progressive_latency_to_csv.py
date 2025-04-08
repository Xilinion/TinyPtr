import os
import re
import csv


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


def main():
    # Base directory for results
    base_dir = "/home/xt253/TinyPtr/results"
    output_dir = os.path.join(base_dir, "csv")

    print(f"Processing results from {base_dir}")
    print(f"Output will be saved to {output_dir}")

    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)

    # List to store all data entries
    all_data = []

    # Define valid IDs based on the updated bash script
    valid_case_ids = [9, 10]
    valid_object_ids = [4, 6, 7, 12, 15]
    load_factors = [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 0.95]

    # Process only the valid result files
    for case_id in valid_case_ids:
        for object_id in valid_object_ids:
            entry_id = 0  # Starting entry_id
            for load_factor in load_factors:
                filename = f"object_{object_id}_case_{case_id}_entry_{entry_id}_.txt"
                file_path = os.path.join(base_dir, filename)

                if os.path.exists(file_path):
                    # Read throughput data
                    throughput, latency = extract_throughput(file_path)

                    # Skip if we couldn't extract the data
                    if throughput is None or latency is None:
                        print(
                            f"Warning: Could not extract throughput data from {filename}")
                        continue

                    # Store the data with load_factor
                    all_data.append(
                        (case_id, object_id, load_factor, throughput, latency))

                entry_id += 1  # Increment entry_id for each load_factor

    # Write a single CSV file
    csv_filename = "progressive_latency_results.csv"
    csv_path = os.path.join(output_dir, csv_filename)

    with open(csv_path, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        # Write header
        writer.writerow(['case_id', 'object_id', 'load_factor', 'throughput (ops/s)', 'latency (ns/op)'])
        # Write data
        for entry in all_data:
            writer.writerow(entry)

    print(f"Created {csv_path}")


if __name__ == "__main__":
    main()
