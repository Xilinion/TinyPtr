# Succinct and Fast Tiny Pointer Hash Tables

## Abstract

Hash tables sit on the critical path of many systems, yet modern designs still force a trade-off between fast operations and high memory overhead. We revisit this trade-off and present Tiny Pointer Hash Tables (TPHT), a family of practical hash tables that make two ideas from theory work at system scale: compressing pointers down to a byte, and encoding keys compactly so less metadata is needed. We engineer these ideas into two complementary designs. Chained-TPHT targets maximal space savings, demonstrating that a real hash table can have a total footprint at or even below the raw size of the stored data. Flattened-TPHT targets latency, organizing data to keep the common case within a single cache miss while retaining strong space efficiency. Both variants support dynamic resizing without global pauses and integrate cleanly with 64-bit keys and values.

Across YCSB and microbenchmarks, TPHT advances the latency-space Pareto frontier: Chained-TPHT reaches 105.4% space efficiency, and Flattened-TPHT achieves 83.4% space efficiency with up to 89.3% higher throughput than strong baselines. Together, these results show that techniques once confined to theory can be turned into production-ready hash tables that meaningfully reduce memory use while delivering state-of-the-art performance.

---

## Artifact Evaluation Guide

### System Requirements

To reproduce all experiments without modification, your system should meet the following specifications:

- **Memory**: 100 GB RAM (minimum)
- **Storage**: 500 GB SSD
- **CPU**: 20+ cores with Intel instruction set support:
  - AVX-512 (AVX-512F, AVX-512BW, AVX-512VL, VBMI)
  - AVX2, POPCNT
  - VBMI2 (preferred but optional)
- **Privileges**: `sudo` access required for configuring paging policies and collecting low-level performance metrics

### Quick Setup

Initialize the build environment and dependencies (~half day):

```bash
bash setup.sh
```

### Reproducing Results

**Step 1: Initialize submodules**

```bash
git submodule update --init --recursive
```

**Step 2: Run all benchmarks** (~2-3 days)

```bash
bash run_all.sh
```

This will:
- Execute all experimental configurations
- Generate raw data in `results/`
- Process results into CSV format in `latex/csv/`

**Note**: While we've included automatic retry logic, some third-party baseline implementations may occasionally encounter issues. We recommend spot-checking key results after the full run completes.

**Step 3: Generate figures**

```bash
cd latex
latexmk -pdf -interaction=nonstopmode -halt-on-error main.tex
```

The generated `main.pdf` uses the **exact same scripts and data processing pipeline** as our paper submission, ensuring full reproducibility of our results. You may notice minor formatting differences due to conference template requirements, and we've included a few additional experimental results beyond the paper for interested readers.

### Exploring Further

We've designed the codebase to be modular and extensible. Here's where to look for customization:

- **Benchmark configurations**: `scripts/benchmark.sh`
- **Test case definitions**: `src/benchmark/benchmark_case_type.h`
- **Object type definitions**: `src/benchmark/benchmark_object_type.h`
- **Core implementations**: `src/benchmark/benchmark.cpp`

We welcome researchers to build upon this work and explore new directions!

---

## Get in Touch

Have questions? Found something interesting? We'd love to hear from you! Feel free to open an issue on GitHub or reach out to the authors directly. Whether you're reproducing our results, extending the implementation, or just curious about the techniques, we're here to help.
