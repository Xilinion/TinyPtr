import os
import re
import csv

import config_py


ENTRY_ID = 10
OBJECT_IDS = [18, 25]  # 18: without staggering, 25: with staggering
CASE_IDS = [17, 18, 19, 20, 21, 22]
CSV_FILENAME = "ycsb_resizing_staggering_results.csv"


def extract_throughput(file_path):
    """Extract Fill Throughput and Run Throughput from a result file."""
    with open(file_path, "r") as f:
        content = f.read()

    fill_match = re.search(r"Fill Throughput: (\d+) ops/s", content)
    run_match = re.search(r"Run Throughput: (\d+) ops/s", content)

    fill_throughput = int(fill_match.group(1)) if fill_match else None
    run_throughput = int(run_match.group(1)) if run_match else None

    return fill_throughput, run_throughput


def main():
    base_dir = config_py.RESULTS_DIR
    output_dir = config_py.CSV_OUTPUT_DIR

    os.makedirs(output_dir, exist_ok=True)

    all_data = []
    for case_id in CASE_IDS:
        for object_id in OBJECT_IDS:
            filename = (
                f"object_{object_id}_case_{case_id}_entry_{ENTRY_ID}_.txt"
            )
            file_path = os.path.join(base_dir, filename)
            if not os.path.exists(file_path):
                continue

            fill_throughput, run_throughput = extract_throughput(file_path)
            if fill_throughput is None or run_throughput is None:
                print(f"Warning: Missing throughput in {filename}")
                continue

            all_data.append(
                (case_id, ENTRY_ID, object_id, fill_throughput, run_throughput)
            )

    csv_path = os.path.join(output_dir, CSV_FILENAME)
    with open(csv_path, "w", newline="") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(
            [
                "case_id",
                "entry_id",
                "object_id",
                "fill_throughput (ops/s)",
                "run_throughput (ops/s)",
            ]
        )
        for entry in all_data:
            writer.writerow(entry)

    print(f"Created {csv_path}")


if __name__ == "__main__":
    main()
