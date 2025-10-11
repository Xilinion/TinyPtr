# Succinct and Fast Tiny Pointer Hash Tables

## Abstract

Hash tables are a ubiquitous data structure across all kinds of systems, because they promise exceptionally low latency for all operations. However, state-of-the-art hash tables still require a costly tradeoff between latency and memory overhead. This is because practical schemes for collision resolution either consume memory or increase latency: the pointers used in chained lists take up space and incur additional cache misses, whereas open-addressing hash tables need to run at a relatively low load factor in order to achieve low latency.

This paper presents two types of Tiny Pointer Hash Tables (TPHT), both of which eliminate this tradeoff by fusing cutting-edge theory with hardware-conscious engineering. In fact, one of these hash tables, Chained-TPHT, through the use of quotienting, even achieves sub-dataset-size memory usage, 105.4% space efficiency, for the first time in practice, while still delivering excellent latency. The second, Flattened-TPHT, offers state-of-the-art latency while still maintaining excellent space efficiency 83.4%. What enables this performance are the following three new ideas: (1) practical byte-sized tiny pointers using power-of-two-choice dereference tables, (2) a practical implementation of quotienting to reduce the space used to encode keys, and (3) careful cache-friendly layouts to ensure that latency isn't compromised. The first two of these ideas exist in the theoretical literature, but require new ideas to be implemented efficiently in practice. The third requires engineering together with a careful load-balancing analysis.

Our evaluation shows that TPHT greatly improves the memory-latency tradeoff: Chained-TPHT's 105.4% space efficiency slashes memory consumption by 38.5% compared to state-of-the-art systems, while Flattened-TPHT delivers 64.3% higher throughput with 83.4% superior space efficiency. Our implementation and experimental scripts are fully accessible at https://github.com/Xilinion/TinyPtr.

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
