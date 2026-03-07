import os
import re
import csv
import config_py


def _parse_int(match_obj):
    if not match_obj:
        return None
    return int(match_obj.group(1).replace(",", ""))


def extract_metrics(file_path):
    with open(file_path, "r") as f:
        content = f.read()

    throughput = _parse_int(re.search(r"Throughput:\s*([\d,]+)\s*ops/s", content))
    latency = _parse_int(re.search(r"Latency:\s*([\d,]+)\s*ns/op", content))

    cpu_time_ms = None
    cpu_match = re.search(r"CPU Time:\s*([\d\.]+)\s*ms", content)
    if cpu_match:
        cpu_time_ms = float(cpu_match.group(1))

    return throughput, latency, cpu_time_ms


def main():
    base_dir = config_py.RESULTS_DIR
    output_dir = config_py.CSV_OUTPUT_DIR
    os.makedirs(output_dir, exist_ok=True)

    rows = []

    tinypointers_comparison_ids = [35, 32, 33, 34]
    valid_case_ids = [1, 3, 6, 7]

    for case_id in valid_case_ids:
        entry_id = 3000  # reset per case as in benchmark.sh
        for object_id in tinypointers_comparison_ids:
            fname = f"object_{object_id}_case_{case_id}_entry_{entry_id}_.txt"
            file_path = os.path.join(base_dir, fname)
            if os.path.exists(file_path):
                throughput, latency, cpu_time_ms = extract_metrics(file_path)
                if throughput is not None:
                    rows.append(
                        {
                            "case_id": case_id,
                            "object_id": object_id,
                            "entry_id": entry_id,
                            "throughput": throughput,
                            "latency": latency,
                            "cpu_time_ms": cpu_time_ms,
                        }
                    )

    rows.sort(key=lambda r: (r["case_id"], r["object_id"]))

    csv_path = os.path.join(output_dir, "tinypointers_comparison_results.csv")
    with open(csv_path, "w", newline="") as csvfile:
        writer = csv.DictWriter(
            csvfile,
            fieldnames=[
                "case_id",
                "object_id",
                "throughput",
            ],
            extrasaction="ignore",
        )
        writer.writeheader()
        writer.writerows(rows)

    print(f"Wrote {csv_path} with {len(rows)} rows.")


if __name__ == "__main__":
    main()
