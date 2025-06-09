import os
import re
import numpy as np
import matplotlib.pyplot as plt

def parse_file(file_path):
    with open(file_path, 'r') as file:
        content = file.read()
    
    # Extract bin size and load factor using regex
    bin_size_match = re.search(r'Bin Size: (\d+)', content)
    load_factor_match = re.search(r'Load Factor: ([\d.]+)', content)
    
    if bin_size_match and load_factor_match:
        bin_size = int(bin_size_match.group(1))
        load_factor = float(load_factor_match.group(1))
        return bin_size, load_factor
    else:
        return None, None

def calculate_bit_used(bin_size):
    return 1 + np.log2(bin_size + 1)

def plot_histogram(data, output_path):
    bin_sizes, load_factors = zip(*data)
    bit_used = [calculate_bit_used(b) for b in bin_sizes]

    fig, ax = plt.subplots()
    bars = ax.bar(bit_used, load_factors, tick_label=bit_used)

    # Set x-ticks with two lines: Bit Used and Bin Size, and make them smaller
    ax.set_xticks(bit_used)
    ax.set_xticklabels([f'{b}\n(Bin Size: {bs})' for b, bs in zip(bit_used, bin_sizes)], fontsize=8)

    ax.set_xlabel('Bit Used')
    ax.set_ylabel('Load Factor (%)')
    ax.set_title('Load Factor vs. Bit Used for Different Bin Sizes(Insert Only)')

    # Annotate each bar with the load factor value
    for bar, load_factor in zip(bars, load_factors):
        yval = bar.get_height()
        ax.text(bar.get_x() + bar.get_width()/2, yval, f'{load_factor:.2f}%', ha='center', va='bottom')

    # Save the plot as a PDF
    plt.savefig(output_path)
    plt.close()

def main():
    base_path = '/home/xt253/TinyPtr/results/'
    file_pattern = 'object_4_case_0_entry_{}_'
    data = []

    for entry_number in range(6):
        file_path = os.path.join(base_path, file_pattern.format(entry_number) + '.txt')
        bin_size, load_factor = parse_file(file_path)
        if bin_size is not None and load_factor is not None:
            data.append((bin_size, load_factor))

    if data:
        output_path = '/home/xt253/TinyPtr/results/figure/load_cap_insert_only.pdf'
        plot_histogram(data, output_path)
        print(f"Plot saved to {output_path}")
    else:
        print("No valid data found.")

if __name__ == "__main__":
    main()
