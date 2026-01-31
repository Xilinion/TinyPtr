import os

import matplotlib.pyplot as plt
import pandas as pd

import config_py


CSV_FILENAME = "ycsb_resizing_staggering_results.csv"
OUTPUT_FILENAME = "ycsb_resizing_staggering_throughput.pdf"

OBJECT_LABELS = {
    18: "TPHT (no staggering)",
    25: "TPHT (staggering)",
}

OBJECT_STYLES = {
    18: {"color": (31 / 255, 119 / 255, 180 / 255)},
    25: {"color": (255 / 255, 127 / 255, 14 / 255)},
}

# Map x positions to (case_id, metric)
PLOT_POINTS = [
    (0, 17, "fill_throughput"),
    (1, 17, "run_throughput"),
    (2, 18, "run_throughput"),
    (3, 19, "run_throughput"),
    (4, 22, "run_throughput"),
]

X_LABELS = ["Load", "Run A", "Run B", "Run C", r"Run C$^-$"]


def plot_resizing_staggering(csv_path, output_dir):
    df = pd.read_csv(csv_path)

    fig, ax = plt.subplots(figsize=(7.5, 2.2))
    bar_width = 0.18
    x_positions = [p[0] for p in PLOT_POINTS]
    offsets = {
        18: -bar_width / 2,
        25: bar_width / 2,
    }

    bar_containers = []
    for object_id in OBJECT_LABELS:
        heights = []
        for x, case_id, metric in PLOT_POINTS:
            row = df[
                (df["case_id"] == case_id) & (df["object_id"] == object_id)
            ]
            if row.empty:
                heights.append(0.0)
                continue
            value = float(row.iloc[0][f"{metric} (ops/s)"])
            heights.append(value / 1e6)

        shifted_x = [x + offsets[object_id] for x in x_positions]
        style = OBJECT_STYLES.get(object_id, {})
        bars = ax.bar(
            shifted_x,
            heights,
            width=bar_width,
            label=OBJECT_LABELS.get(object_id, str(object_id)),
            **style,
        )
        bar_containers.append(bars)

    ax.set_ylabel("Throughput (M/s)")
    ax.set_xticks(x_positions)
    ax.set_xticklabels(X_LABELS)
    ax.set_ylim(bottom=0)
    ax.grid(axis="y", color="0.85", linestyle="-", linewidth=0.6)
    ax.set_axisbelow(True)
    ax.legend(frameon=False, ncol=2, loc="upper center")

    for bars in bar_containers:
        ax.bar_label(bars, fmt="%.1f", padding=2, fontsize=7, rotation=90)

    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, OUTPUT_FILENAME)
    fig.tight_layout()
    fig.savefig(output_path, format="pdf", dpi=300)
    plt.close(fig)
    print(f"Saved {output_path}")


def main():
    csv_path = os.path.join(config_py.CSV_OUTPUT_DIR, CSV_FILENAME)
    output_dir = os.path.join(config_py.RESULTS_DIR, "figure")

    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"Missing CSV file: {csv_path}")

    plot_resizing_staggering(csv_path, output_dir)


if __name__ == "__main__":
    main()
