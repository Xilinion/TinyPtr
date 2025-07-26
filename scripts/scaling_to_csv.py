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
    valid_object_ids = [6, 7, 15, 17, 20]
    valid_case_ids = [1, 3, 6, 7]
    thread_nums = [1, 2, 4, 8, 16, 32]

    # Process only the valid result files
    for case_id in valid_case_ids:
        for object_id in valid_object_ids:
            entry_id = 10  # Starting entry_id
            for thread_num in thread_nums:
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

                    # Store the data
                    all_data.append(
                        (case_id, object_id, thread_num, throughput, latency))

                entry_id += 1  # Increment entry_id for each thread_num

    # Write a single CSV file
    csv_filename = "scaling_results.csv"
    csv_path = os.path.join(output_dir, csv_filename)

    with open(csv_path, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        # Write header
        writer.writerow(['case_id', 'object_id', 'thread_num', 'throughput (ops/s)', 'latency (ns/op)'])
        # Write data
        for entry in all_data:
            writer.writerow(entry)

    print(f"Created {csv_path}")


if __name__ == "__main__":
    main()
