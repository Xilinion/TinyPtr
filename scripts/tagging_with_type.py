import os
import csv
import config_py  # Import the shared configuration

# Mapping from case_id to names
case_id_map = {
    0: "INSERT_ONLY_LOAD_FACTOR_SUPPORT",
    1: "INSERT_ONLY",
    2: "UPDATE_ONLY",
    3: "ERASE_ONLY",
    4: "ALTERNATING_INSERT_ERASE",
    5: "ALL_OPERATION_RAND",
    6: "QUERY_HIT_ONLY",
    7: "QUERY_MISS_ONLY",
    8: "QUERY_HIT_PERCENT",
    9: "QUERY_HIT_ONLY_CUSTOM_LOAD_FACTOR",
    10: "QUERY_MISS_ONLY_CUSTOM_LOAD_FACTOR",
    11: "QUERY_HIT_PERCENT_CUSTOM_LOAD_FACTOR",
    12: "XXHASH64_THROUGHPUT",
    13: "PRNG_THROUGHPUT",
    14: "LATENCY_VARYING_CHAINLENGTH",
    15: "QUERY_NO_MEM",
    16: "INSERT_DELETE_LOAD_FACTOR_SUPPORT",
    17: "YCSB_A",
    18: "YCSB_B",
    19: "YCSB_C",
    20: "YCSB_NEG_A",
    21: "YCSB_NEG_B",
    22: "YCSB_NEG_C",
}

# Mapping from object_id to names
object_id_map = {
    0: "DEREFTAB64",
    1: "CHAINEDHT64",
    2: "INTARRAY64",
    3: "STDUNORDEREDMAP64",
    4: "BYTEARRAYCHAINEDHT",
    5: "CLHT",
    6: "CUCKOO",
    7: "ICEBERG",
    8: "BINAWARECHAINEDHT",
    9: "SAMEBINCHAINEDHT",
    10: "YARDEDTTPHT",
    11: "FLATHASH",
    12: "SKULKERHT",
    13: "GROWT",
    14: "CONCURRENT_SKULKERHT",
    15: "JUNCTION",
    16: "RESIZABLE_SKULKERHT",
    17: "CONCURRENT_BYTEARRAYCHAINEDHT",
    18: "RESIZABLE_BYTEARRAYCHAINEDHT",
    19: "BOLT",
    20: "BLAST",
    21: "RESIZABLE_BLAST",
}

def tag_csv_files():
    # Base directory for CSV files
    base_dir = config_py.CSV_OUTPUT_DIR
    output_dir = os.path.join(base_dir, "../tagged_csv")

    print(f"Processing CSV files from {base_dir}")
    print(f"Tagged CSV files will be saved to {output_dir}")

    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)

    # Process each CSV file in the base directory
    for filename in os.listdir(base_dir):
        if filename.endswith(".csv"):
            file_path = os.path.join(base_dir, filename)
            tagged_file_path = os.path.join(output_dir, filename)

            with open(file_path, 'r', newline='') as csvfile:
                reader = csv.DictReader(csvfile)
                fieldnames = reader.fieldnames

                # Check if 'case_id' and 'object_id' are in the CSV
                if 'case_id' in fieldnames and 'object_id' in fieldnames:
                    with open(tagged_file_path, 'w', newline='') as tagged_csvfile:
                        writer = csv.DictWriter(tagged_csvfile, fieldnames=fieldnames)
                        writer.writeheader()

                        for row in reader:
                            # Replace case_id and object_id with their names
                            row['case_id'] = case_id_map.get(int(row['case_id']), row['case_id'])
                            row['object_id'] = object_id_map.get(int(row['object_id']), row['object_id'])
                            writer.writerow(row)

            print(f"Tagged {filename} and saved to {tagged_file_path}")

if __name__ == "__main__":
    tag_csv_files()
