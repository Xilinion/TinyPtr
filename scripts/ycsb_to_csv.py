import os
import re
import csv
import config_py  # Import the shared configuration


def extract_throughput(file_path):
    """Extract Fill Throughput and Run Throughput from a result file."""
    with open(file_path, 'r') as f:
        content = f.read()

        fill_throughput_match = re.search(
            r'Fill Throughput: (\d+) ops/s', content)
        run_throughput_match = re.search(
            r'Run Throughput: (\d+) ops/s', content)

        fill_throughput = int(fill_throughput_match.group(
            1)) if fill_throughput_match else None
        run_throughput = int(run_throughput_match.group(1)
                             ) if run_throughput_match else None

        return fill_throughput, run_throughput


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

    # Define valid IDs based on the bash script
    valid_ids = {
        0: [6, 7, 15, 17, 20],  # entry_id = 0
        1: [6, 7, 15, 18, 21]   # entry_id = 1
    }
    valid_case_ids = [17, 18, 19, 20, 21, 22]

    # Process only the valid result files
    for entry_id, object_ids in valid_ids.items():
        for case_id in valid_case_ids:
            for object_id in object_ids:
                filename = f"object_{object_id}_case_{case_id}_entry_{entry_id}_.txt"
                file_path = os.path.join(base_dir, filename)

                if os.path.exists(file_path):
                    # Read throughput data
                    fill_throughput, run_throughput = extract_throughput(
                        file_path)

                    # Skip if we couldn't extract the data
                    if fill_throughput is None or run_throughput is None:
                        print(
                            f"Warning: Could not extract throughput data from {filename}")
                        continue

                    # Store the data
                    all_data.append(
                        (case_id, entry_id, object_id, fill_throughput, run_throughput))

    # Write a single CSV file
    csv_filename = "ycsb_results.csv"
    csv_path = os.path.join(output_dir, csv_filename)

    with open(csv_path, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        # Write header
        writer.writerow(['case_id', 'entry_id', 'object_id', 'fill_throughput (ops/s)', 'run_throughput (ops/s)'])
        # Write data
        for entry in all_data:
            writer.writerow(entry)

    print(f"Created {csv_path}")


if __name__ == "__main__":
    main()
