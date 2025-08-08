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
    """Extract occupancy distribution data from a result file."""
    occupancy_data = []
    
    with open(file_path, 'r') as f:
        lines = f.readlines()
    
    # Find the start of occupancy data
    start_idx = -1
    operation_count = None
    
    for i, line in enumerate(lines):
        if "Operation Count:" in line:
            operation_count = int(line.split(":")[1].strip())
        elif "Bin Occupancy, Count:" in line:
            start_idx = i + 1
            break
    
    if start_idx == -1:
        return None, None
    
    # Extract occupancy data
    for i in range(start_idx, len(lines)):
        line = lines[i].strip()
        if not line:  # Empty line marks end of data
            break
        
        try:
            parts = line.split(", ")
            if len(parts) == 2:
                occupancy = int(parts[0])
                count = int(parts[1])
                occupancy_data.append((occupancy, count))
        except ValueError:
            continue
    
    return occupancy_data, operation_count


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
    
    # Dictionary to collect experimental data by occupancy level
    experimental_data = {}  # occupancy -> list of probabilities
    
    # Process occupancy distribution files based on benchmark.sh pattern
    # case_id=23, object_id=22, entry_id=0 to 9 (10 repetitions)
    case_id = 23
    object_id = 22
    
    for entry_id in range(NUM_REPETITIONS):  # 0 to 9
        filename = f"object_{object_id}_case_{case_id}_entry_{entry_id}_.txt"
        file_path = os.path.join(base_dir, filename)
        
        if os.path.exists(file_path):
            occupancy_data, operation_count = extract_occupancy_data(file_path)
            
            if occupancy_data and operation_count:
                total_bins = sum(count for _, count in occupancy_data)
                
                # print(f"Processing {filename}: operation_count = {operation_count}, total_bins = {total_bins}")
                
                for occupancy, count in occupancy_data:
                    # Calculate experimental probability
                    exp_probability = count / total_bins if total_bins > 0 else 0
                    
                    # Collect data by occupancy level
                    if occupancy not in experimental_data:
                        experimental_data[occupancy] = []
                    experimental_data[occupancy].append(exp_probability)
            else:
                print(f"Warning: Could not extract data from {filename}")
        else:
            print(f"Warning: File not found: {filename}")
    
    # Lists to store data entries
    all_data = []  # combined (back-compat)
    experimental_box_rows = []  # occupancy, median, lower, upper, lower_whisker, upper_whisker
    poisson_rows = []  # occupancy, probability
    
    # Add experimental box plot data
    for occupancy in sorted(experimental_data.keys()):
        probabilities = experimental_data[occupancy]
        # Fill missing repetitions with 0
        while len(probabilities) < NUM_REPETITIONS:
            probabilities.append(0.0)
        
        # Calculate box plot statistics
        stats = calculate_boxplot_stats(probabilities)
        if stats:
            all_data.append([
                object_id, case_id, occupancy, 0,  # object_id, case_id, occupancy, count
                0, stats['median'],  # total_bins, median
                stats['upper'], stats['lower'],  # upper, lower
                stats['upper_whisker'], stats['lower_whisker'],  # upper_whisker, lower_whisker
                occupancy  # draw_position
            ])
            experimental_box_rows.append([
                occupancy,
                stats['median'],
                stats['lower'],
                stats['upper'],
                stats['lower_whisker'],
                stats['upper_whisker']
            ])
            # all_data.append([
            #     object_id, case_id, occupancy, 0,  # object_id, case_id, occupancy, count
            #     0, 0,  # total_bins, median
            #     0, 0,  # upper, lower
            #     0, 0  # upper_whisker, lower_whisker
            # ])
    
    # Add theoretical Poisson data points (lambda=1) with fine granularity
    lambda_val = 1.0
    for i in range(0, 101):  # 101 points from 0 to 10
        occupancy = i / 10.0  # Steps of 0.1 from 0 to 10
        poisson_prob = calculate_poisson_probability(occupancy, lambda_val)
        all_data.append([
            -1, -1, occupancy, 0,  # object_id=-1, case_id=-1, occupancy, count
            0, poisson_prob,  # total_bins, exp_probability
            0, 0, 0, 0,  # upper, lower, upper_whisker, lower_whisker (not used for theoretical)
            occupancy  # draw_position (not used for theoretical)
        ])
        poisson_rows.append([occupancy, poisson_prob])
    
    # Write CSV file
    csv_filename = "occupancy_results.csv"
    csv_path = os.path.join(output_dir, csv_filename)
    
    with open(csv_path, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        # Write header
        writer.writerow([
            'object_id', 'case_id', 'occupancy', 'count', 
            'total_bins', 'exp_probability', 'upper', 'lower', 'upper_whisker', 'lower_whisker',
            'draw_position'
        ])
        # Write data with regular float notation
        for entry in all_data:
            # Convert scientific notation to regular float notation
            formatted_entry = []
            for i, value in enumerate(entry):
                if isinstance(value, float):
                    formatted_entry.append(f"{value:.15f}")
                else:
                    formatted_entry.append(value)
            writer.writerow(formatted_entry)
    
    print(f"Created {csv_path}")
    print(f"Total data points: {len(all_data)}")

    # Write simplified box plot CSV - exactly 12 rows (0-11), each with safe values
    box_csv = os.path.join(output_dir, "occupancy_experimental_box.csv")
    with open(box_csv, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['occupancy', 'median', 'lower', 'upper', 'lower_whisker', 'upper_whisker'])
        # Ensure exactly 12 rows for occupancy 0-11, fill missing with safe defaults
        experimental_box_dict = {o: (med, low, up, lw, uw) for o, med, low, up, lw, uw in experimental_box_rows}
        for occ in range(12):  # 0 to 11
            if occ in experimental_box_dict:
                med, low, up, lw, uw = experimental_box_dict[occ]
                # Clamp to safe positive values for log scale
                med = max(med, 1e-12)
                low = max(low, 1e-12)
                up = max(up, 1e-12)
                lw = max(lw, 1e-12)
                uw = max(uw, 1e-12)
            else:
                # Default safe values for missing occupancy levels
                med = low = up = lw = uw = 1e-12
            writer.writerow([f"{occ}", f"{med:.15f}", f"{low:.15f}", f"{up:.15f}", f"{lw:.15f}", f"{uw:.15f}"])

    poisson_csv = os.path.join(output_dir, "occupancy_poisson.csv")
    with open(poisson_csv, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['x', 'y'])
        for x, y in poisson_rows:
            writer.writerow([f"{x}", f"{y:.15f}"])

    print(f"Created {box_csv}")
    print(f"Created {poisson_csv}")
    
    # Print summary statistics
    if all_data:
        experimental_data_points = [row for row in all_data if row[0] != -1]
        theoretical_data_points = [row for row in all_data if row[0] == -1]
        print(f"Experimental box plot data points: {len(experimental_data_points)}")
        print(f"Theoretical data points: {len(theoretical_data_points)}")
        print(f"Added theoretical Poisson data points (lambda=1)")


if __name__ == "__main__":
    main()
