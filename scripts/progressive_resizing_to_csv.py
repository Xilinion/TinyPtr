import os
import csv
import config_py


def parse_progressive_file(path):
    rows = []
    with open(path, "r") as f:
        lines = f.readlines()
    # Skip first two header lines, parse subsequent CSV-like rows: window, ops, cpu, throughput, latency
    for line in lines[2:]:
        parts = [p.strip() for p in line.split(",")]
        if len(parts) < 4:
            continue
        try:
            window = int(parts[0])
            throughput = int(parts[3])
        except ValueError:
            continue
        rows.append((window, throughput))
    return rows


def main():
    base_dir = config_py.RESULTS_DIR
    output_dir = config_py.CSV_OUTPUT_DIR
    os.makedirs(output_dir, exist_ok=True)

    case_id = 29
    object_ids = [6, 7, 15, 18, 21, 24, 25]  # rss_object_ids from benchmark.sh
    entry_id = 2000  # fixed for progressive resizing block

    out_rows = []

    for object_id in object_ids:
        fname = f"object_{object_id}_case_{case_id}_entry_{entry_id}_.txt"
        fpath = os.path.join(base_dir, fname)
        if not os.path.exists(fpath):
            print(f"Warning: missing {fname}")
            continue
        for window, throughput in parse_progressive_file(fpath):
            out_rows.append(
                {
                    "case_id": case_id,
                    "object_id": object_id,
                    "window": window,
                    "throughput (ops/s)": throughput,
                }
            )

    out_rows.sort(key=lambda r: (r["object_id"], r["window"]))

    csv_path = os.path.join(output_dir, "progressive_resizing_results.csv")
    with open(csv_path, "w", newline="") as csvfile:
        writer = csv.DictWriter(
            csvfile,
            fieldnames=["case_id", "object_id", "window", "throughput (ops/s)"],
        )
        writer.writeheader()
        writer.writerows(out_rows)

    print(f"Created {csv_path} with {len(out_rows)} rows.")


if __name__ == "__main__":
    main()
