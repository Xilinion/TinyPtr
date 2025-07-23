import sys

if __name__ == "__main__":
    if len(sys.argv) < 4:
        sys.exit("Usage: python ycsb_neg.py <ycsb_load_file> <ycsb_exe_file> <ycsb_neg_exe_file>")

    ycsb_load_file = sys.argv[1]
    ycsb_exe_file = sys.argv[2]
    ycsb_neg_exe_file = sys.argv[3]

    # Read the ycsb_load_file and extract keys for INSERT operations
    with open(ycsb_load_file, 'r') as f:
        load_keys = [int(line.strip().split()[1]) for line in f if line.strip().upper().startswith("INSERT")]

    # Determine the starting point for new keys
    new_key = max(load_keys) + 1

    # Process the exe file
    with open(ycsb_exe_file, 'r') as f:
        exe_lines = f.readlines()

    # For READ operations, replace the key with new_key and increment new_key
    neg_lines = []
    for line in exe_lines:
        parts = line.strip().split()
        if parts[0].upper() == "READ":
            neg_lines.append(f"{parts[0]} {new_key}\n")
            new_key += 1
        else:
            neg_lines.append(line)

    # Write the result to ycsb_neg_exe_file
    with open(ycsb_neg_exe_file, 'w') as f:
        f.writelines(neg_lines)