import os
import re
import glob

# Define the directory containing the performance files
perf_dir = "/home/xt253/TinyPtr/results/"

# Define the performance metrics to extract
metrics = [
    "task-clock",
    "context-switches",
    "cpu-migrations",
    "page-faults",
    "cycles",
    "instructions",
    "branches",
    "branch-misses",
    "L1-dcache-loads",
    "L1-dcache-load-misses",
    "L1-icache-load-misses",
    "branch-load-misses",
    "branch-loads",
    "LLC-loads",
    "LLC-load-misses",
    "dTLB-loads",
    "dTLB-load-misses",
    "cache-misses",
    "cache-references"
]

# Map test case IDs to their names
test_case_names = {
    1: "INSERT_ONLY",
    2: "UPDATE_ONLY",
    3: "ERASE_ONLY",
    4: "ALTERNATING_INSERT_ERASE",
    6: "QUERY_HIT_ONLY",
    7: "QUERY_MISS_ONLY",
}

# Function to parse a performance file and extract metrics
def parse_perf_file(file_path):
    with open(file_path, 'r') as file:
        content = file.read()
    
    perf_data = {}
    perf_percent = {}
    for metric in metrics:
        match = re.search(rf"([\d,\.]+)\s+{metric}", content)
        
        if match:
            perf_data[metric] = match.group(1)
            after_match = re.search(rf"{match.group(1)}.*(#.*)", content)
            if after_match:
                perf_percent[metric] = after_match.group(1)
    
    return perf_data, perf_percent
    

    # with open(file_path, 'r') as file:
    #     content = file.readlines()
    # with open(file_path, 'r') as file:
    #     content = file.readlines()
    
    # perf_data = {}
    # for line in content:
    #     match = re.match(r"^\s*([\d,\.]+)\s+([\w\-]+(?:\s[\w\-]+)*)\s+#\s+(.+)$", line)
    #     if match:
    #         value, metric, comment = match.groups()
    #         perf_data[metric.strip()] = (value.replace(',', ''), comment.strip())
    #     else:
    #         match = re.match(r"^\s*([\d,\.]+)\s+([\w\-]+(?:\s[\w\-]+)*)$", line)
    #         if match:
    #             value, metric = match.groups()
    #             perf_data[metric.strip()] = (value.replace(',', ''), "")
    
    # return perf_data

# Function to get the test case ID from the file name
def get_test_case_id(file_name):
    match = re.search(r"case_(\d+)_", file_name)
    return int(match.group(1)) if match else None

def get_entry_id(file_name):
    match = re.search(r"entry_(\d+)_", file_name)
    return int(match.group(1)) if match else None

def get_object_id(file_name):
    match = re.search(r"object_(\d+)_", file_name)
    return int(match.group(1)) if match else None

def get_if_perf(file_name):
    match = re.search(r"perf", file_name)
    return True if match else False
    

# Get all performance files
perf_files = glob.glob(os.path.join(perf_dir, "*.txt"))

# print(perf_files)


# Parse all performance files and store the results
results = {}
for perf_file in perf_files:
    if not get_if_perf(perf_file):
        continue
    if get_object_id(perf_file) != 4:
        continue
    if get_entry_id(perf_file) != 0:
        continue
    test_case_id = get_test_case_id(perf_file)
    if test_case_id in test_case_names:
        print(f"Processing {perf_file}")
        results[test_case_id] = parse_perf_file(perf_file)
        print(results[test_case_id])

# Save the comparison results to a text file
with open('performance_comparison.txt', 'w') as file:
    # Write the header
    header = f"{'Metric':<25} {'Test Case':<29} {'Value':>15} {'Percent':<30}\n"
    file.write(header)
    file.write(f"{'=' * 25:<25} {'=' * 29:<29} {'=' * 15} {'=' * 30}\n")
    
    # Write the data for each metric and test case
    for metric in metrics:
        for test_case_id in results.keys():
            perf_data, perf_percent = results[test_case_id]
            test_case_name = test_case_names.get(test_case_id, "UNKNOWN")
            value = perf_data.get(metric, "N/A")
            percent = perf_percent.get(metric, "N/A")
            line = f"{metric:<25} {test_case_name:<29} {value:>15} {percent:<30}\n"
            file.write(line)
        line = f"{'-' * 25:<25} {'-' * 29:<29} {'-' * 15} {'-' * 30}\n"
        file.write(line)