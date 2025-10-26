import os
import re
import csv
import config_py  # shared configuration


def extract_throughput(file_path: str, case_id: int):
    """Extract Throughput from a result file. Returns int ops/s or None."""
    if not os.path.exists(file_path):
        return None
    with open(file_path, 'r') as f:
        content = f.read()
    
    # For cases 9 and 10, extract Query metrics specifically
    if case_id in [9, 10]:
        m = re.search(r'Query Throughput: (\d+) ops/s', content)
    else:
        # For other cases (like case 1), use the old format
        m = re.search(r'Throughput: (\d+) ops/s', content)
    
    return int(m.group(1)) if m else None


def main():
    base_dir = config_py.RESULTS_DIR
    output_dir = config_py.CSV_OUTPUT_DIR

    print(f"Processing data-size scaling results from {base_dir}")
    os.makedirs(output_dir, exist_ok=True)

    # Cases and objects used in data-size scaling (see benchmark.sh)
    case_ids = [1, 3, 9, 10]
    object_ids = [6, 7, 15, 17, 20, 24]

    # Entry id range and sizes order as defined in benchmark.sh
    entry_start = 1000
    table_sizes = [32767, 262143, 2097151, 16777215, 134217727]

    rows = []
    for case_id in case_ids:
        for object_id in object_ids:
            for idx, table_size in enumerate(table_sizes):
                entry_id = entry_start + idx
                filename = f"object_{object_id}_case_{case_id}_entry_{entry_id}_.txt"
                file_path = os.path.join(base_dir, filename)
                thr = extract_throughput(file_path, case_id)
                if thr is None:
                    print(f"Warning: missing throughput in {filename}")
                    continue
                rows.append([case_id, object_id, table_size, thr])

    # Write CSV
    csv_path = os.path.join(output_dir, "data_size_scaling_results.csv")
    with open(csv_path, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["case_id", "object_id", "table_size", "throughput (ops/s)"])
        for r in rows:
            writer.writerow(r)

    print(f"Created {csv_path}")


if __name__ == "__main__":
    main()


