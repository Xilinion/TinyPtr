import os
import re
import csv
import config_py


def extract_throughput(file_path, case_id):
    with open(file_path, "r") as f:
        content = f.read()

    if case_id in (9, 10):
        throughput_match = re.search(
            r"Query Throughput:\s*([\d,]+)\s*ops/s", content)
        latency_match = re.search(
            r"Query Latency:\s*([\d,]+)\s*ns/op", content)
    else:
        throughput_match = re.search(
            r"Throughput:\s*([\d,]+)\s*ops/s", content)
        latency_match = re.search(r"Latency:\s*([\d,]+)\s*ns/op", content)

    throughput = int(throughput_match.group(1).replace(
        ",", "")) if throughput_match else None
    latency = int(latency_match.group(1).replace(
        ",", "")) if latency_match else None
    return throughput, latency


def extract_memory_usage(file_path):
    real_values = []
    virtual_values = []

    with open(file_path, "r") as f:
        for line in f:
            match = re.match(
                r"\s*\d+\.\d+\s+\d+\.\d+\s+([\d.]+)\s+([\d.]+)", line)
            if match:
                real_values.append(float(match.group(1)))
                virtual_values.append(float(match.group(2)))

    if not real_values or not virtual_values:
        return 0.0, 0.0

    real_range = max(real_values) - min(real_values)
    virtual_range = max(virtual_values) - min(virtual_values)
    return real_range, virtual_range


def main():
    base_dir = config_py.RESULTS_DIR
    output_dir = config_py.CSV_OUTPUT_DIR
    os.makedirs(output_dir, exist_ok=True)

    collected_rows = []

    valid_case_ids = [1, 9, 10]
    valid_object_ids = [4, 23, 26, 27, 28, 29, 30, 31]
    load_factors = [0.05, 0.1, 0.15, 0.2, 0.25, 0.3, 0.35, 0.4, 0.45,
                    0.5, 0.55, 0.6, 0.65, 0.7, 0.75, 0.8, 0.85, 0.9, 0.95, 0.99]
    num_repetitions = 10  # per benchmark.sh for compact hash tables

    for case_id in valid_case_ids:
        for object_id in valid_object_ids:
            base_entry_id = 4000

            for load_factor in load_factors:
                entry_id = base_entry_id

                memuse_filename = (
                    f"object_{object_id}_case_{case_id}_entry_{entry_id}_memuse.txt"
                )
                memuse_file_path = os.path.join(base_dir, memuse_filename)
                if os.path.exists(memuse_file_path):
                    real_range, virtual_range = extract_memory_usage(
                        memuse_file_path)
                else:
                    real_range, virtual_range = 0.0, 0.0

                throughputs = []
                latencies = []
                for rep in range(num_repetitions):
                    entry_id = base_entry_id + rep
                    filename = (
                        f"object_{object_id}_case_{case_id}_entry_{entry_id}_.txt"
                    )
                    file_path = os.path.join(base_dir, filename)
                    if os.path.exists(file_path):
                        throughput, latency = extract_throughput(
                            file_path, case_id)
                        if throughput is None or latency is None:
                            print(
                                "Warning: missing throughput/latency for "
                                f"{filename}"
                            )
                            continue
                        throughputs.append(throughput)
                        latencies.append(latency)

                if throughputs:
                    avg_throughput = sum(throughputs) / len(throughputs)
                    avg_latency = sum(latencies) / len(latencies)
                    collected_rows.append(
                        {
                            "case_id": case_id,
                            "object_id": object_id,
                            "load_factor": load_factor,
                            "throughput (ops/s)": avg_throughput,
                            "latency (ns/op)": avg_latency,
                            "real_memory (MB)": real_range,
                            "virtual_memory (MB)": virtual_range,
                        }
                    )

                base_entry_id += num_repetitions

    virtual_mem_case1 = {}
    for row in collected_rows:
        if row["case_id"] == 1:
            virtual_mem_case1[(row["object_id"], row["load_factor"])] = row[
                "virtual_memory (MB)"
            ]

    csv_filename = "throughput_space_eff_compact_results.csv"
    csv_path = os.path.join(output_dir, csv_filename)

    with open(csv_path, "w", newline="") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(
            [
                "case_id",
                "object_id",
                "load_factor",
                "throughput (ops/s)",
                "latency (ns/op)",
                "real_memory (MB)",
                "virtual_memory (MB)",
                "virtual_memory_case1 (MB)",
                "space_efficiency",
                "throughput_millions",
            ]
        )

        for row in collected_rows:
            key = (row["object_id"], row["load_factor"])
            virtual_case1 = virtual_mem_case1.get(
                key, row["virtual_memory (MB)"]
            )

            if virtual_case1 > 0:
                space_eff = 256 * row["load_factor"] / virtual_case1
            else:
                space_eff = 0.0

            writer.writerow(
                [
                    row["case_id"],
                    row["object_id"],
                    row["load_factor"],
                    row["throughput (ops/s)"],
                    row["latency (ns/op)"],
                    row["real_memory (MB)"],
                    row["virtual_memory (MB)"],
                    virtual_case1,
                    space_eff,
                    row["throughput (ops/s)"] / 1_000_000,
                ]
            )

    print(f"Created {csv_path}")
    print(f"Total data points: {len(collected_rows)}")


if __name__ == "__main__":
    main()
