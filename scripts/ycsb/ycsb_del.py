import sys

if __name__ == "__main__":
    if len(sys.argv) < 3:
        sys.exit("Usage: python ycsb_del.py <ycsb_load_file> <ycsb_del_file>")

    ycsb_load_file = sys.argv[1]
    ycsb_del_file = sys.argv[2]

    # Read the ycsb_load_file and extract keys for INSERT operations
    # We'll generate DELETE operations for the same keys in the same order
    del_lines = []
    
    with open(ycsb_load_file, 'r') as f:
        for line in f:
            parts = line.strip().split()
            if len(parts) >= 2 and parts[0].upper() == "INSERT":
                # Replace INSERT with DELETE, keep the same key
                del_lines.append(f"DELETE {parts[1]}\n")

    # Write the result to ycsb_del_file
    with open(ycsb_del_file, 'w') as f:
        f.writelines(del_lines)
    
    print(f"Generated {len(del_lines)} DELETE operations from {ycsb_load_file}")
    print(f"Output written to {ycsb_del_file}")

