import os
import pandas as pd
import matplotlib.pyplot as plt

# Define the directory where the results are stored
results_dir = '../results'
output_dir = '../results/figure'

# Ensure the output directory exists
os.makedirs(output_dir, exist_ok=True)

# Define the case and object IDs to process
case_ids = [1, 6, 7]
object_ids = [3, 4, 6, 7, 10, 12]
table_sizes = [1000000, 2000000, 4000000, 8000000, 16000000, 32000000, 64000000, 128000000]

# Mapping of case IDs to names
case_id_to_name = {
    1: "INSERT_ONLY",
    6: "QUERY_HIT_ONLY",
    7: "QUERY_MISS_ONLY"
}

# Mapping of object IDs to names
object_id_to_name = {
    3: "STDUNORDEREDMAP64",
    4: "BYTEARRAYCHAINEDHT",
    6: "CUCKOO",
    7: "ICEBERG",
    10: "YARDEDTTPHT",
    12: "SKULKERHT"
}

# Initialize a list to store the data
data = []

# Iterate over each combination of case_id, object_id, and table_size
for case_id in case_ids:
    if case_id == 5:
        continue
    for object_id in object_ids:
        entry_id = 0
        for table_size in table_sizes:
            # Construct the filename
            filename = f'object_{object_id}_case_{case_id}_entry_{entry_id}_.txt'
            filepath = os.path.join(results_dir, filename)
            
            # Check if the file exists
            if os.path.exists(filepath):
                # Read the file and extract the data
                with open(filepath, 'r') as file:
                    lines = file.readlines()
                    latency = int(lines[2].split(': ')[1].strip().split()[0])
                    
                    # Append the data to the list
                    data.append({
                        'case_id': case_id,
                        'object_id': object_id,
                        'table_size': table_size,
                        'latency': latency
                    })
            
            entry_id += 1

# Check if data is collected
if not data:
    print("No data collected. Please check the file paths and contents.")
else:
    # Convert the data to a DataFrame
    df = pd.DataFrame(data)
    print("DataFrame columns:", df.columns)

    # Plot the data for each case_id separately
    for case_id in case_ids:
        plt.figure(figsize=(12, 8))
        for object_id in object_ids:
            subset = df[(df['case_id'] == case_id) & (df['object_id'] == object_id)]
            case_name = case_id_to_name.get(case_id, f"Case {case_id}")
            object_name = object_id_to_name.get(object_id, f"Object {object_id}")
            plt.plot(subset['table_size'], subset['latency'], marker='o', label=f'{object_name}')

        plt.xscale('log')
        plt.xlabel('Table Size')
        plt.ylabel('Latency (ns/op)')
        plt.title(f'Benchmark Latency vs Table Size for {case_name}')
        plt.legend()
        plt.grid(True)

        # Save the plot to a PDF file
        output_path = os.path.join(output_dir, f'{case_name}.pdf')
        plt.savefig(output_path)
        print(f"Plot for {case_name} saved to {output_path}")
        plt.close()