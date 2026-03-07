import os

import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import pandas as pd

import config_py


CSV_FILENAME = "throughput_space_eff_compact_results.csv"

OBJECT_LABELS = {
    4: "TPHT",
    23: "Blast",
    26: "Bucket",
    27: "Group",
    28: r"Cleary$^{plain}$",
    29: r"Cleary$^{sparse}$",
    30: r"Layered$^{plain}$",
    31: r"Layered$^{sparse}$",
}

OBJECT_STYLES = {
    4: {"color": (44 / 255, 160 / 255, 44 / 255), "marker": "D"},
    23: {"color": (255 / 255, 127 / 255, 14 / 255), "marker": "p"},
    26: {"color": (31 / 255, 119 / 255, 180 / 255), "marker": "o"},
    27: {"color": (214 / 255, 39 / 255, 40 / 255), "marker": "s"},
    28: {"color": (127 / 255, 127 / 255, 127 / 255), "marker": "^"},
    29: {"color": (148 / 255, 103 / 255, 189 / 255), "marker": "*"},
    30: {"color": (102 / 255, 194 / 255, 165 / 255), "marker": "o"},
    31: {"color": (231 / 255, 138 / 255, 195 / 255), "marker": "x"},
}

CASES = {
    1: "Insertion",
    9: "Positive Query",
    10: "Negative Query",
}

FIGURES = [
    ("cleary", [4, 23, 28, 29]),
    ("layered", [4, 23, 30, 31]),
    ("bucket", [4, 23, 26]),
    ("group", [4, 23, 27]),
]


def style_axis(ax):
    ax.set_xlabel("Space Efficiency", fontsize=9)
    ax.set_xlim(left=0)
    ax.set_ylim(bottom=0)
    ax.grid(True, which="major", color="0.8", linestyle="-", linewidth=0.6)
    ax.tick_params(labelsize=8)
    ax.xaxis.set_major_locator(mticker.MultipleLocator(0.2))
    ax.xaxis.set_major_formatter(mticker.FormatStrFormatter("%.2f"))


def plot_case(ax, df_case, object_ids):
    y_max = 0.0
    for object_id in object_ids:
        data = df_case[df_case["object_id"] == object_id].sort_values(
            "space_efficiency"
        )
        if data.empty:
            continue
        y_max = max(y_max, data["throughput_millions"].max())
        style = OBJECT_STYLES.get(object_id, {})
        ax.plot(
            data["space_efficiency"],
            data["throughput_millions"],
            label=OBJECT_LABELS.get(object_id, str(object_id)),
            linewidth=1.6,
            markersize=4,
            **style,
        )
        if object_id in {4, 28}:
            tail = data.tail(5)
            base_offset = (10, 12) if object_id == 4 else (-18, -14)
            for idx, (_, row) in enumerate(tail.iterrows()):
                jitter = (0, (idx - 2) * 6)
                ax.annotate(
                    f"{row['throughput_millions']:.1f}",
                    (row["space_efficiency"], row["throughput_millions"]),
                    textcoords="offset points",
                    xytext=(base_offset[0] + jitter[0], base_offset[1] + jitter[1]),
                    fontsize=4,
                    color=style.get("color", "black"),
                    bbox={"boxstyle": "round,pad=0.1", "fc": "white", "ec": "none", "alpha": 0.7},
                )
    if y_max > 0:
        ax.set_ylim(0, y_max * 1.05)


def plot_compact_variant(csv_path, output_dir):
    df = pd.read_csv(csv_path)

    for key, object_ids in FIGURES:
        fig, axes = plt.subplots(1, 3, figsize=(7.5, 2.2), sharey=False)

        for ax, (case_id, title) in zip(axes, CASES.items()):
            df_case = df[df["case_id"] == case_id]
            plot_case(ax, df_case, object_ids)
            style_axis(ax)
            ax.set_title(title, fontsize=9)

        axes[0].set_ylabel("Throughput (M/s)", fontsize=9)

        handles, labels = axes[0].get_legend_handles_labels()
        fig.legend(
            handles,
            labels,
            loc="upper center",
            ncol=len(object_ids),
            frameon=False,
            fontsize=8,
        )

        fig.tight_layout(rect=[0, 0, 1, 0.85])
        output_path = os.path.join(
            output_dir, f"throughput_space_eff_compact_{key}.pdf"
        )
        fig.savefig(output_path, format="pdf", dpi=300)
        plt.close(fig)
        print(f"Saved {output_path}")


def main():
    csv_path = os.path.join(config_py.CSV_OUTPUT_DIR, CSV_FILENAME)
    output_dir = os.path.join(config_py.RESULTS_DIR, "figure")
    os.makedirs(output_dir, exist_ok=True)

    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"Missing CSV file: {csv_path}")

    plot_compact_variant(csv_path, output_dir)


if __name__ == "__main__":
    main()
