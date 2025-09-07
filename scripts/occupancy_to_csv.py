import os
import re
import csv
import math
import numpy as np
from scipy.special import gamma
import config_py  # Import the shared configuration


def calculate_poisson_probability(k, lambda_val):
    """Calculate Poisson probability P(X=k) for given lambda."""
    if k == int(k):  # Integer value
        return (lambda_val**k * math.exp(-lambda_val)) / math.factorial(int(k))
    else:  # Non-integer value, use precise Gamma function
        return (lambda_val**k * math.exp(-lambda_val)) / gamma(k + 1)


def calculate_boxplot_stats(values):
    """Calculate box plot statistics for a list of values."""
    if not values:
        return None
    
    values = np.array(values)
    q1 = np.percentile(values, 25)
    q3 = np.percentile(values, 75)
    median = np.median(values)
    
    # Calculate whiskers (1.5 * IQR rule)
    iqr = q3 - q1
    lower_whisker = max(q1 - 1.5 * iqr, np.min(values))
    upper_whisker = min(q3 + 1.5 * iqr, np.max(values))
    
    return {
        'median': median,
        'upper': q3,
        'lower': q1,
        'upper_whisker': upper_whisker,
        'lower_whisker': lower_whisker
    }


def extract_occupancy_data(file_path):
    """Extract occupancy distribution data from a result file for all key types."""
    all_data = {}  # key_type -> (occupancy_data, operation_count)
    
    with open(file_path, 'r') as f:
        lines = f.readlines()
    
    current_key_type = None
    i = 0
    
    while i < len(lines):
        line = lines[i].strip()
        
        # Check for key type headers
        if line.startswith("=== ") and line.endswith(" ==="):
            current_key_type = line[4:-4]  # Extract key type name
            i += 1
            continue
        
        # Look for operation count and occupancy data
        if current_key_type and "Operation Count:" in line:
            operation_count = int(line.split(":")[1].strip())
            i += 1
            
            # Look for "Bin Occupancy, Count:" header
            while i < len(lines) and "Bin Occupancy, Count:" not in lines[i]:
                i += 1
            
            if i < len(lines):
                i += 1  # Skip the header line
                occupancy_data = []
                
                # Extract occupancy data until empty line
                while i < len(lines):
                    data_line = lines[i].strip()
                    if not data_line:  # Empty line marks end of this section
                        break
                    
                    try:
                        parts = data_line.split(", ")
                        if len(parts) == 2:
                            occupancy = int(parts[0])
                            count = int(parts[1])
                            occupancy_data.append((occupancy, count))
                    except ValueError:
                        pass
                    
                    i += 1
                
                all_data[current_key_type] = (occupancy_data, operation_count)
        
        i += 1
    
    return all_data


def main():
    # Use the shared configuration
    base_dir = config_py.RESULTS_DIR
    output_dir = config_py.CSV_OUTPUT_DIR
    
    # Configurable parameter for number of repetitions
    NUM_REPETITIONS = 10
    
    print(f"Processing occupancy results from {base_dir}")
    print(f"Output will be saved to {output_dir}")
    print(f"Number of repetitions: {NUM_REPETITIONS}")
    
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    # Dictionary to collect experimental data by key type and occupancy level
    experimental_data_by_key_type = {}  # key_type -> occupancy -> list of probabilities
    
    # Process occupancy distribution files based on benchmark.sh pattern
    # case_id=23, object_id=22, entry_id=0 to 9 (10 repetitions)
    case_id = 23
    object_id = 22
    
    for entry_id in range(NUM_REPETITIONS):  # 0 to 9
        filename = f"object_{object_id}_case_{case_id}_entry_{entry_id}_.txt"
        file_path = os.path.join(base_dir, filename)
        
        if os.path.exists(file_path):
            all_key_data = extract_occupancy_data(file_path)
            
            if all_key_data:
                print(f"Processing {filename}: Found {len(all_key_data)} key types")
                
                for key_type, (occupancy_data, operation_count) in all_key_data.items():
                    if key_type not in experimental_data_by_key_type:
                        experimental_data_by_key_type[key_type] = {}
                    
                    total_bins = sum(count for _, count in occupancy_data)
                    
                    for occupancy, count in occupancy_data:
                        # Calculate experimental probability
                        exp_probability = count / total_bins if total_bins > 0 else 0
                        
                        # Collect data by key type and occupancy level
                        if occupancy not in experimental_data_by_key_type[key_type]:
                            experimental_data_by_key_type[key_type][occupancy] = []
                        experimental_data_by_key_type[key_type][occupancy].append(exp_probability)
            else:
                print(f"Warning: Could not extract data from {filename}")
        else:
            print(f"Warning: File not found: {filename}")
    
    # Generate CSV files for each key type
    key_type_mapping = {
        "Random Keys": "random",
        "Sequential Keys": "sequential", 
        "Low Hamming Weight Keys": "low_hamming",
        "High Hamming Weight Keys": "high_hamming"
    }
    
    # Add theoretical Poisson data points (lambda=1) with fine granularity
    lambda_val = 1.0
    poisson_rows = []
    for i in range(0, 101):  # 101 points from 0 to 10
        occupancy = i / 10.0  # Steps of 0.1 from 0 to 10
        poisson_prob = calculate_poisson_probability(occupancy, lambda_val)
        poisson_rows.append([occupancy, poisson_prob])
    
    # Write Poisson theoretical data (same for all key types)
    poisson_csv = os.path.join(output_dir, "occupancy_poisson.csv")
    with open(poisson_csv, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['x', 'y'])
        for x, y in poisson_rows:
            writer.writerow([f"{x}", f"{y:.15f}"])
    print(f"Created {poisson_csv}")
    
    # Process each key type
    for key_type, experimental_data in experimental_data_by_key_type.items():
        if key_type not in key_type_mapping:
            print(f"Warning: Unknown key type '{key_type}', skipping")
            continue
            
        key_suffix = key_type_mapping[key_type]
        print(f"\nProcessing key type: {key_type} -> {key_suffix}")
        
        # Lists to store data entries
        all_data = []  # combined (back-compat)
        experimental_box_rows = []  # occupancy, median, lower, upper, lower_whisker, upper_whisker
        
        # Add experimental box plot data
        experimental_box_rows = []
        for occupancy in sorted(experimental_data.keys()):
            probabilities = experimental_data[occupancy]
            # Fill missing repetitions with 0
            while len(probabilities) < NUM_REPETITIONS:
                probabilities.append(0.0)
            
            # Calculate box plot statistics
            stats = calculate_boxplot_stats(probabilities)
            if stats:
                experimental_box_rows.append([
                    occupancy,
                    stats['median'],
                    stats['lower'],
                    stats['upper'],
                    stats['lower_whisker'],
                    stats['upper_whisker']
                ])

        # Write simplified box plot CSV - exactly 12 rows (0-11), with actual zeros where appropriate
        box_csv = os.path.join(output_dir, f"occupancy_experimental_box_{key_suffix}.csv")
        with open(box_csv, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(['occupancy', 'median', 'lower', 'upper', 'lower_whisker', 'upper_whisker'])
            # Ensure exactly 12 rows for occupancy 0-11
            experimental_box_dict = {o: (med, low, up, lw, uw) for o, med, low, up, lw, uw in experimental_box_rows}
            for occ in range(12):  # 0 to 11
                if occ in experimental_box_dict:
                    med, low, up, lw, uw = experimental_box_dict[occ]
                    # Keep actual values, including zeros
                    writer.writerow([f"{occ}", f"{med:.15f}", f"{low:.15f}", f"{up:.15f}", f"{lw:.15f}", f"{uw:.15f}"])
                else:
                    # Missing occupancy levels should be actual zeros, not tiny values
                    writer.writerow([f"{occ}", "0.000000000000000", "0.000000000000000", "0.000000000000000", "0.000000000000000", "0.000000000000000"])
        
        print(f"Created {box_csv}")
    
    print(f"\nSummary:")
    print(f"Processed {len(experimental_data_by_key_type)} key types: {list(experimental_data_by_key_type.keys())}")
    print(f"Generated CSV files for each key type with theoretical Poisson comparison")


if __name__ == "__main__":
    main()
