#!/usr/bin/env python3

import math
import random
import sys
from typing import List, Tuple
try:
    import matplotlib.pyplot as plt
except Exception:
    plt = None

# Editable configuration
# Change the values below to adjust the simulation
PARTITIONS = 8                  # K: number of partitions
INITIAL_SIZE = 1000             # L: initial capacity per partition (same for all)
NUM_INSERTS = 100000            # n: total number of inserts
RESIZE_FACTOR = 2.0             # E: multiplier when resizing (> 1.0)
RESIZE_THRESHOLD = 0.7         # threshold as fraction of capacity (0, 1]
DISTRIBUTION = "uniform"        # "uniform" or "zipf"
ZIPF_S = 1.0                    # Zipf exponent s (only if DISTRIBUTION == "zipf")
SEED = 0                        # PRNG seed for reproducibility
VERBOSE = False                 # Print resize events
SAVE_PLOT = True                # Save a plot of memory growth
PLOT_PATH = "./mem_growth.png"    # Output path for the plot
SIZE_INIT_MODES = {
    0: "constant",  # all partitions start at A
    1: "linear",    # evenly increase from A to E*A across partitions
    2: "exp",       # A * E^(i/(K-1)) across partitions
}
SIZE_INIT_MODE_ID = 2           # choose one of the keys in SIZE_INIT_MODES
SIZE_INIT_MODE_NAME = SIZE_INIT_MODES.get(SIZE_INIT_MODE_ID, "constant")
PLOT_ALL_MODES = True           # Plot all SIZE_INIT_MODES on the same figure


def build_zipf_weights(k: int, s: float) -> List[float]:
    # Prob(i) ~ 1/(i+1)^s, normalized over i in [0..k-1]
    weights = [1.0 / ((i + 1) ** s) for i in range(k)]
    total = sum(weights)
    return [w / total for w in weights]


def initial_capacities(k: int, initial_size: int, resize_factor: float, mode: str) -> List[int]:
    if k <= 0:
        return []
    if mode == "constant" or k == 1:
        return [initial_size for _ in range(k)]
    if mode == "linear":
        # Evenly increase from A to E*A (inclusive) across partitions 0..k-1
        a = float(initial_size)
        b = float(initial_size) * float(resize_factor)
        return [max(1, int(round(a + (b - a) * (i / (k - 1))))) for i in range(k)]
    if mode == "exp":
        # Geometric growth: A * E^(i/(k-1)) for i in [0..k-1]
        a = float(initial_size)
        e = float(resize_factor)
        return [max(1, int(round(a * (e ** (i / (k - 1)))))) for i in range(k)]
    return [initial_size for _ in range(k)]


def simulate(
    k: int,
    initial_size: int,
    n_inserts: int,
    resize_factor: float,
    resize_threshold: float,
    distribution: str,
    zipf_s: float,
    rng: random.Random,
    verbose: bool,
    mode_name: str = None,
) -> Tuple[float, float, int, int, List[int], List[int], List[Tuple[int, int]]]:
    if mode_name is None:
        mode_name = SIZE_INIT_MODE_NAME
    capacities: List[int] = initial_capacities(k, initial_size, resize_factor, mode_name)
    counts: List[int] = [0 for _ in range(k)]

    # Precompute threshold counts per partition
    thresholds: List[int] = [max(1, int(math.floor(resize_threshold * c))) for c in capacities]

    if distribution == "zipf":
        weights = build_zipf_weights(k, zipf_s)
    else:
        weights = None

    def choose_partition() -> int:
        if distribution == "uniform":
            return rng.randrange(k)
        return rng.choices(range(k), weights=weights, k=1)[0]

    total_allocated = sum(capacities)
    growth_points: List[Tuple[int, int]] = []
    # record initial baseline
    growth_points.append((0, total_allocated))
    if verbose:
        print(f"init_total_allocated={total_allocated}")

    for i in range(1, n_inserts + 1):
        pid = choose_partition()

        # Resize if exceeding threshold
        if counts[pid] + 1 > thresholds[pid]:
            old_cap = capacities[pid]
            new_cap = max(old_cap + 1, int(math.floor(old_cap * resize_factor)))
            capacities[pid] = new_cap
            thresholds[pid] = max(1, int(math.floor(resize_threshold * new_cap)))
            total_allocated += (new_cap - old_cap)
            if verbose:
                print(f"resize step={i} part={pid} old_cap={old_cap} new_cap={new_cap} total_alloc={total_allocated}")
            growth_points.append((i, total_allocated))

        counts[pid] += 1

    total_inserted = n_inserts
    total_allocated = sum(capacities)

    avg_eff = total_inserted / total_allocated if total_allocated > 0 else 0.0
    worst_eff = min((counts[j] / capacities[j]) if capacities[j] > 0 else 0.0 for j in range(k))

    return avg_eff, worst_eff, total_inserted, total_allocated, counts, capacities, growth_points


def compute_time_efficiencies(n_inserts: int, growth_points: List[Tuple[int, int]]) -> Tuple[float, float]:
    if n_inserts <= 0:
        return 0.0, 0.0
    if not growth_points:
        return 0.0, 0.0

    gp = list(growth_points)
    if gp[-1][0] != n_inserts:
        gp.append((n_inserts, gp[-1][1]))

    def sum_to(m: int) -> int:
        return m * (m + 1) // 2

    total_eff_sum = 0.0
    worst_eff = float('inf')

    prev_x, prev_alloc = gp[0]
    for idx, (x, alloc) in enumerate(gp[1:], start=1):
        if x > prev_x:
            # Sum i from prev_x+1 to x, divided by constant allocation
            s = sum_to(x) - sum_to(prev_x)
            total_eff_sum += s / prev_alloc
            # Worst within the segment is at the first insertion of the segment
            # Skip the first segment (before the first resize) for worst calculation
            if idx > 1:
                worst_eff = min(worst_eff, (prev_x + 1) / prev_alloc)
        prev_x, prev_alloc = x, alloc

    avg_eff_time = total_eff_sum / n_inserts
    if worst_eff == float('inf'):
        worst_eff = 0.0
    return avg_eff_time, worst_eff


def main(argv: List[str]) -> int:
    k = PARTITIONS
    initial_size = INITIAL_SIZE
    n_inserts = NUM_INSERTS
    resize_factor = RESIZE_FACTOR
    resize_threshold = RESIZE_THRESHOLD
    distribution = DISTRIBUTION
    zipf_s = ZIPF_S
    seed = SEED
    verbose = VERBOSE

    if k <= 0:
        print("error: --partitions must be > 0", file=sys.stderr)
        return 2
    if initial_size <= 0:
        print("error: --initial-size must be > 0", file=sys.stderr)
        return 2
    if n_inserts < 0:
        print("error: --num-inserts must be >= 0", file=sys.stderr)
        return 2
    if resize_factor <= 1.0:
        print("error: --resize-factor must be > 1.0", file=sys.stderr)
        return 2
    if not (0.0 < resize_threshold <= 1.0):
        print("error: --resize-threshold must be in (0, 1]", file=sys.stderr)
        return 2

    rng = random.Random(seed)

    # Decide which modes to simulate
    if PLOT_ALL_MODES:
        modes_to_run = sorted(SIZE_INIT_MODES.items())
    else:
        modes_to_run = [(SIZE_INIT_MODE_ID, SIZE_INIT_MODE_NAME)]

    results = {}
    for mode_id, mode_name in modes_to_run:
        rng_local = random.Random(seed)
        avg_eff, worst_eff, total_inserted, total_allocated, counts, capacities, growth_points = simulate(
            k,
            initial_size,
            n_inserts,
            resize_factor,
            resize_threshold,
            distribution,
            zipf_s,
            rng_local,
            verbose,
            mode_name,
        )
        avg_eff_time, worst_eff_time = compute_time_efficiencies(n_inserts, growth_points)
        results[mode_name] = {
            "avg_eff": avg_eff,
            "worst_eff": worst_eff,
            "total_inserted": total_inserted,
            "total_allocated": total_allocated,
            "counts": counts,
            "capacities": capacities,
            "growth_points": growth_points,
            "avg_eff_time": avg_eff_time,
            "worst_eff_time": worst_eff_time,
        }

        print(f"\n=== Simulation Summary ({mode_name}) ===")
        print(f"partitions={k}")
        print(f"initial_size_per_partition={initial_size}")
        print(f"num_inserts={total_inserted}")
        print(f"resize_factor={resize_factor}")
        print(f"resize_threshold={resize_threshold}")
        print(f"distribution={distribution}")
        if distribution == "zipf":
            print(f"zipf_s={zipf_s}")
        print(f"size_init_policy_name={mode_name}")
        print(f"total_allocated={total_allocated}")
        print(f"final_space_efficiency={avg_eff:.6f}")
        print(f"avg_space_efficiency_time={avg_eff_time:.6f}")
        print(f"worst_space_efficiency_time={worst_eff_time:.6f}")

        # Compact per-partition stats
        print("partition_id,inserted,capacity,efficiency")
        for pid, (cnt, cap) in enumerate(zip(counts, capacities)):
            eff_p = (cnt / cap) if cap > 0 else 0.0
            print(f"{pid},{cnt},{cap},{eff_p:.6f}")

    # Plot memory growth if requested
    if SAVE_PLOT:
        if plt is None:
            print("warning: matplotlib not available; cannot save plot", file=sys.stderr)
        else:
            fig, ax1 = plt.subplots(figsize=(10, 5))

            colors = ['#1f77b4', '#ff7f0e', '#2ca02c']
            linestyles = ['-', '--', '-.']

            mem_lines = []
            eff_lines = []

            for idx, mode_name in enumerate(sorted(results.keys())):
                res = results[mode_name]
                growth_points = res["growth_points"]
                
                # Ensure final point extends to total inserts
                xs: List[int] = []
                ys: List[int] = []
                if not growth_points:
                    growth_points = [(0, sum(res["capacities"]))]

                # Start from first point
                for x, y in growth_points:
                    xs.append(x)
                    ys.append(y)
                if growth_points[-1][0] != n_inserts:
                    xs.append(n_inserts)
                    ys.append(growth_points[-1][1])

                # Step plot with horizontal segments between changes (memory)
                mem_line, = ax1.step(xs, ys, where='post', 
                                     label=f'{mode_name} (mem)',
                                     color=colors[idx % len(colors)],
                                     linestyle=linestyles[idx % len(linestyles)],
                                     linewidth=1.5)
                mem_lines.append(mem_line)

            # y = x reference (perfect space efficiency)
            diag_x = [0, n_inserts]
            diag_y = [0, n_inserts]
            diag_line, = ax1.plot(diag_x, diag_y, linestyle=':', color='gray', 
                                  label='y = x (perfect)', linewidth=1.5)

            ax1.set_xlabel('#inserted')
            ax1.set_ylabel('current mem use (total capacity)')
            title_suffix = f' (all modes)' if PLOT_ALL_MODES else f' ({SIZE_INIT_MODE_NAME} init)'
            ax1.set_title(f'Memory growth and space efficiency{title_suffix}')
            ax1.grid(True, linestyle='--', alpha=0.4)

            # Efficiency on secondary y-axis
            ax2 = ax1.twinx()

            for idx, mode_name in enumerate(sorted(results.keys())):
                res = results[mode_name]
                growth_points = res["growth_points"]
                
                xs_eff: List[int] = []
                ys_eff: List[float] = []
                gp_ext = list(growth_points)
                if gp_ext[-1][0] != n_inserts:
                    gp_ext.append((n_inserts, gp_ext[-1][1]))
                for j in range(len(gp_ext) - 1):
                    x0, alloc0 = gp_ext[j]
                    x1, alloc1 = gp_ext[j + 1]
                    # Linear increase with constant allocation between resizes
                    xs_eff.extend([x0, x1])
                    ys_eff.extend([
                        0.0 if alloc0 == 0 else x0 / alloc0,
                        0.0 if alloc0 == 0 else x1 / alloc0,
                    ])
                    # Vertical drop at the resize boundary to reflect new allocation
                    if alloc1 != alloc0:
                        xs_eff.append(x1)
                        ys_eff.append(0.0 if alloc1 == 0 else x1 / alloc1)
                eff_line, = ax2.plot(xs_eff, ys_eff, 
                                     label=f'{mode_name} (eff)',
                                     color=colors[idx % len(colors)],
                                     linestyle=linestyles[idx % len(linestyles)],
                                     alpha=0.7,
                                     linewidth=1.2)
                eff_lines.append(eff_line)

            ax2.set_ylabel('space efficiency')
            ax2.set_ylim(0.0, 1.05)

            # Combined legend
            lines = mem_lines + [diag_line] + eff_lines
            labels = [l.get_label() for l in lines]
            ax1.legend(lines, labels, loc='best', fontsize=8, ncol=2)

            fig.tight_layout()
            fig.savefig(PLOT_PATH)
            print(f"\nplot_saved={PLOT_PATH}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))


