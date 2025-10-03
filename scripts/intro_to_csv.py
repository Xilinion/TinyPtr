import os
import csv
from collections import defaultdict
import config_py


def generate_ycsb_summary_csv(ycsb_csv_path: str, output_dir: str) -> str:
    """Generate a CSV summarizing (entry_id=0 only):
    - average load phase throughput (fill_throughput)
    - average run throughput (run_throughput)
    Aggregated per object_id across all case_id rows present.
    """
    by_object_id_fill = defaultdict(list)
    by_object_id_run = defaultdict(list)

    with open(ycsb_csv_path, "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                entry_id = int(row["entry_id"]) if row.get("entry_id") is not None else None
            except ValueError:
                continue

            # Only use entry_id = 0 as requested
            if entry_id != 0:
                continue

            try:
                object_id = int(row["object_id"]) if row.get("object_id") is not None else None
                fill_tp = int(row["fill_throughput (ops/s)"]) if row.get("fill_throughput (ops/s)") else None
                run_tp = int(row["run_throughput (ops/s)"]) if row.get("run_throughput (ops/s)") else None
            except ValueError:
                # Skip rows with invalid numeric values
                continue

            if object_id is None:
                continue

            if fill_tp is not None:
                by_object_id_fill[object_id].append(fill_tp)
            if run_tp is not None:
                by_object_id_run[object_id].append(run_tp)

    # Compute averages per object_id
    summary_rows = []
    object_ids = sorted(set(by_object_id_fill.keys()) | set(by_object_id_run.keys()))
    for object_id in object_ids:
        fill_values = by_object_id_fill.get(object_id, [])
        run_values = by_object_id_run.get(object_id, [])
        if not fill_values and not run_values:
            continue

        avg_fill = sum(fill_values) / len(fill_values) if fill_values else 0.0
        avg_run = sum(run_values) / len(run_values) if run_values else 0.0

        summary_rows.append({
            "object_id": object_id,
            "avg_load_throughput_fill (ops/s)": avg_fill,
            "avg_run_throughput (ops/s)": avg_run,
        })

    # Write output
    os.makedirs(output_dir, exist_ok=True)
    out_path = os.path.join(output_dir, "ycsb_intro_summary.csv")
    with open(out_path, "w", newline="") as out_f:
        writer = csv.DictWriter(
            out_f,
            fieldnames=[
                "object_id",
                "avg_load_throughput_fill (ops/s)",
                "avg_run_throughput (ops/s)",
            ],
        )
        writer.writeheader()
        for row in summary_rows:
            writer.writerow(row)

    return out_path


def generate_max_space_efficiency_csv(space_eff_csv_path: str, output_dir: str) -> str:
    """Generate a CSV containing the maximum space efficiency per object_id."""
    max_space_eff_by_object = {}

    with open(space_eff_csv_path, "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                object_id = int(row["object_id"]) if row.get("object_id") is not None else None
                space_eff = float(row["space_efficiency"]) if row.get("space_efficiency") else None
            except ValueError:
                continue

            if object_id is None or space_eff is None:
                continue

            current_max = max_space_eff_by_object.get(object_id)
            if current_max is None or space_eff > current_max:
                max_space_eff_by_object[object_id] = space_eff

    # Prepare rows
    rows = [
        {"object_id": object_id, "max_space_efficiency": max_space_eff}
        for object_id, max_space_eff in sorted(max_space_eff_by_object.items())
    ]

    # Write output
    os.makedirs(output_dir, exist_ok=True)
    out_path = os.path.join(output_dir, "max_space_efficiency.csv")
    with open(out_path, "w", newline="") as out_f:
        writer = csv.DictWriter(out_f, fieldnames=["object_id", "max_space_efficiency"])
        writer.writeheader()
        for row in rows:
            writer.writerow(row)

    return out_path


def main():
    csv_dir = config_py.CSV_OUTPUT_DIR

    ycsb_csv = os.path.join(csv_dir, "ycsb_results.csv")
    space_eff_csv = os.path.join(csv_dir, "throughput_space_eff_results.csv")

    print(f"Reading YCSB results from: {ycsb_csv}")
    print(f"Reading throughput/space-eff from: {space_eff_csv}")

    ycsb_out = generate_ycsb_summary_csv(ycsb_csv, csv_dir)
    space_eff_out = generate_max_space_efficiency_csv(space_eff_csv, csv_dir)

    print(f"Created {ycsb_out}")
    print(f"Created {space_eff_out}")


if __name__ == "__main__":
    main()


