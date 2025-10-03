import os
import re
import csv
import config_py  # Import the shared configuration

def extract_load_factor(file_path):
    """Extract Load Factor from a result file."""
    with open(file_path, 'r') as f:
        content = f.read()

        load_factor_match = re.search(
            r'Load Factor: ([\d.]+) %', content)

        load_factor = float(load_factor_match.group(
            1)) if load_factor_match else None

        return load_factor


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
    valid_case_ids = [0]
    valid_object_ids = [4]
    bin_sizes = [3, 7, 15, 31, 63, 127]

    # Process only the valid result files
    for case_id in valid_case_ids:
        for object_id in valid_object_ids:
            entry_id = 0  # Starting entry_id
            for bin_size in bin_sizes:
                filename = f"object_{object_id}_case_{case_id}_entry_{entry_id}_.txt"
                file_path = os.path.join(base_dir, filename)

                if os.path.exists(file_path):
                    # Read load factor data
                    load_factor = extract_load_factor(file_path)

                    # Skip if we couldn't extract the data
                    if load_factor is None:
                        print(
                            f"Warning: Could not extract load factor from {filename}")
                        continue

                    # Store the data with bin_size
                    all_data.append(
                        (case_id, object_id, bin_size, load_factor))

                entry_id += 1  # Increment entry_id for each bin_size

    # Write a single CSV file
    csv_filename = "load_factor_support_results.csv"
    csv_path = os.path.join(output_dir, csv_filename)

    with open(csv_path, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        # Write header
        writer.writerow(['case_id', 'object_id', 'bin_size', 'load_factor (%)'])
        # Write data
        for entry in all_data:
            writer.writerow(entry)

    print(f"Created {csv_path}")


if __name__ == "__main__":
    main()
