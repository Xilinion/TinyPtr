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


def main():
    # Use the shared configuration
    base_dir = config_py.RESULTS_DIR
    output_dir = config_py.CSV_OUTPUT_DIR

    print(f"Processing percentile results from {base_dir}")
    print(f"Output will be saved to {output_dir}")

    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)

    # List to store all data entries
    all_data = []

    # Define valid IDs for percentile cases (24, 25)
    valid_case_ids = [24, 25]  # YCSB_A_PERCENTILE, YCSB_NEG_A_PERCENTILE
    valid_object_ids = [6, 7, 15, 18, 21, 24]  # Resizable objects

    # Process only the valid result files
    for case_id in valid_case_ids:
        for object_id in valid_object_ids:
            entry_id = 0  # For percentile cases, entry_id is always 0
            filename = f"object_{object_id}_case_{case_id}_entry_{entry_id}_.txt"
            file_path = os.path.join(base_dir, filename)

            if os.path.exists(file_path):
                # Read percentile data
                percentile_data = extract_percentile_data(file_path)

                # Skip if we couldn't extract the data
                if percentile_data is None:
                    print(f"Warning: Could not extract percentile data from {filename}")
                    continue

                # Store the data
                for operation_type, percentile, latency in percentile_data:
                    all_data.append({
                        'case_id': case_id,
                        'entry_id': entry_id,
                        'object_id': object_id,
                        'operation_type': operation_type,
                        'percentile': percentile,
                        'latency_ns': latency
                    })

    # Write a single CSV file
    csv_filename = "percentile_results.csv"
    csv_path = os.path.join(output_dir, csv_filename)

    with open(csv_path, 'w', newline='') as csvfile:
        fieldnames = ['case_id', 'entry_id', 'object_id', 'operation_type', 'percentile', 'latency_ns']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        
        # Write header
        writer.writeheader()
        
        # Write data
        for entry in all_data:
            writer.writerow(entry)

    print(f"Created {csv_path}")
    print(f"Total records: {len(all_data)}")


if __name__ == "__main__":
    main() 