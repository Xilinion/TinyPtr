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


def main():
    # Use the shared configuration
    base_dir = config_py.RESULTS_DIR
    output_dir = config_py.CSV_OUTPUT_DIR

    print(f"Processing results from {base_dir}")
    print(f"Output will be saved to {output_dir}")

    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)

    # List to store all data entries
    all_data = []

    # Define valid IDs based on the updated bash script
    valid_ids = {
        0: [6, 7, 15, 17, 20]  # entry_id = 0
    }
    valid_case_ids = [1, 3, 6, 7]

    # Process only the valid result files
    for entry_id, object_ids in valid_ids.items():
        for case_id in valid_case_ids:
            for object_id in object_ids:
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

                    # Store the data without entry_id
                    all_data.append(
                        (case_id, object_id, throughput, latency))

    # Write a single CSV file
    csv_filename = "micro_results.csv"
    csv_path = os.path.join(output_dir, csv_filename)

    with open(csv_path, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        # Write header
        writer.writerow(['case_id', 'object_id', 'throughput (ops/s)', 'latency (ns/op)'])
        # Write data
        for entry in all_data:
            writer.writerow(entry)

    print(f"Created {csv_path}")


if __name__ == "__main__":
    main()
