import os
import pandas as pd
import matplotlib.pyplot as plt
import math
import numpy as np
import matplotlib.ticker as ticker

# Directory containing the result files
directory = '/home/xt253/TinyPtr/results/'

# Initialize a list to store data
data = []

# Iterate over each file in the directory
for entry in range(107):  # Assuming entries range from 0 to 106
    file_path = os.path.join(directory, f'object_4_case_16_entry_{entry}_.txt')
    if os.path.exists(file_path):
        with open(file_path, 'r') as file:
            lines = file.readlines()
            # Extract relevant data
            bin_size = int(lines[1].split(': ')[1].strip())
            load_factor = float(lines[2].split(': ')[1].strip())
            op_size_ratio = float(lines[5].split(': ')[1].strip())
            # Append to data list
            data.append({'Bin Size': bin_size, 'Load Factor': load_factor, 'Op/Size Ratio': op_size_ratio})

# Convert data to a DataFrame
df = pd.DataFrame(data)

# Convert Load Factor to percentage
df['Load Factor'] *= 100

# Plotting
plt.figure(figsize=(10, 6))
for bin_size, group in df.groupby('Bin Size'):
    bit_used = int(1 + math.log2(bin_size + 1))
    plt.plot(group['Load Factor'], group['Op/Size Ratio'], 
             label=f'Bit Used: {bit_used}, Bin Size: {bin_size}')
    # Annotate the x-component of the endpoint
    end_point = group.iloc[-1]
    plt.annotate(f"{int(end_point['Load Factor'])}%", 
                 (end_point['Load Factor'], end_point['Op/Size Ratio']),
                 textcoords="offset points", xytext=(5,5), ha='center')

plt.xlabel('Load Factor (%)')
plt.ylabel('Op/Size Ratio')
plt.title('Load Factor vs Op/Size Ratio for Different Bin Sizes (With Deletion)')
plt.yscale('log')  # Ensure y-axis is set to logarithmic scale

# Set more dense y-ticks using LogLocator
plt.gca().yaxis.set_major_locator(ticker.LogLocator(base=10.0, numticks=20))  # Adjust subs and numticks as needed

plt.legend()

# Make grid lines dashed and less opaque
plt.grid(True, alpha=0.5, linestyle='--')  # Adjust alpha for grid line opacity

# Set more dense x-ticks
plt.xticks(np.arange(0, 101, 5))  # Adjust the range and step as needed


# Save the plot to a PDF file
plt.savefig('/home/xt253/TinyPtr/results/figure/load_cap_with_del.pdf')
plt.close()
