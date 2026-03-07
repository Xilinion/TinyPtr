import os
import re
import csv
import config_py


def _parse_int(match_obj):
    if not match_obj:
        return None
    # Remove thousands separators if present
    return int(match_obj.group(1).replace(",", ""))


def extract_metrics(file_path, case_id):
    """Extract the single throughput metric for a file based on case_id."""
    with open(file_path, "r") as f:
        content = f.read()

    throughput = None

    if case_id in (9, 10):
        throughput = _parse_int(
            re.search(r"Query Throughput:\s*([\d,]+)\s*ops/s", content)
        )
    else:
        throughput = _parse_int(
            re.search(r"Throughput:\s*([\d,]+)\s*ops/s", content)
        )

    # Fallbacks if the expected line is missing
    if throughput is None:
        throughput = _parse_int(
            re.search(r"Insert Throughput:\s*([\d,]+)\s*ops/s", content)
        ) or _parse_int(
            re.search(r"Query Throughput:\s*([\d,]+)\s*ops/s", content)
        )

    return throughput


def main():
    base_dir = config_py.RESULTS_DIR
    output_dir = config_py.CSV_OUTPUT_DIR
    os.makedirs(output_dir, exist_ok=True)

    rows = []
    valid_object_ids = [6, 7, 15, 17, 20, 24]
    valid_case_ids = [1, 3, 9, 10]
    thread_nums = [1]  # L1 sized table uses only one thread in benchmark.sh

    for case_id in valid_case_ids:
        for object_id in valid_object_ids:
            entry_id = 5000  # reset per object/case per benchmark.sh loop
            for _ in thread_nums:
                fname = (
                    f"object_{object_id}_case_{case_id}_entry_{entry_id}_.txt"
                )
                file_path = os.path.join(base_dir, fname)
                if os.path.exists(file_path):
                    throughput = extract_metrics(file_path, case_id)
                    if throughput is not None:
                        rows.append(
                            {
                                "case_id": case_id,
                                "object_id": object_id,
                                "throughput": throughput,
                            }
                        )
                entry_id += 1

    rows.sort(key=lambda r: (r["case_id"], r["object_id"]))

    csv_path = os.path.join(output_dir, "small_table_results.csv")
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
