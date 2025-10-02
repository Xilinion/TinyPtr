#!/usr/bin/env python3

"""
Generate a Poisson probability table.

Given a Poisson rate (lambda) and a count n, this script prints n rows
for i = 0..n-1 with columns: i, poisson(i), prefix_sum (CDF up to i),
and tail (1 - prefix_sum).

Usage:
  python scripts/poisson_table.py 3.5 20

This will print 20 rows for i in [0, 19].
"""

from __future__ import annotations

import argparse
import math
import sys
from typing import Iterable, List, Tuple


def generate_poisson_table(rate_lambda: float, num_points: int) -> List[Tuple[int, float, float, float]]:
    """Return rows of (i, pmf, cdf, tail) for i in [0, num_points-1].

    The Poisson PMF is computed iteratively for numerical efficiency:
      p(0) = exp(-lambda), p(i) = p(i-1) * lambda / i
    """
    if rate_lambda <= 0.0:
        raise ValueError("lambda must be > 0")
    if num_points <= 0:
        raise ValueError("num_points must be > 0")

    rows: List[Tuple[int, float, float, float]] = []
    pmf_current: float = math.exp(-rate_lambda)  # p(0)
    cdf: float = 0.0

    for i in range(num_points):
        if i > 0:
            pmf_current *= rate_lambda / i
        cdf += pmf_current
        tail = 1.0 - cdf
        if tail < 0.0 and tail > -1e-15:  # clamp tiny negatives from rounding
            tail = 0.0
        rows.append((i, pmf_current, cdf, tail))

    return rows


def print_table(rows: Iterable[Tuple[int, float, float, float]]) -> None:
    print(f"{'i':>6} {'poisson(i)':>14} {'prefix_sum':>14} {'tail':>14}")
    for i, pmf, cdf, tail in rows:
        print(f"{i:6d} {pmf:14.10f} {cdf:14.10f} {tail:14.10f}")


def parse_args(argv: List[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Print Poisson PMF/CDF table for n points starting at i=0")
    parser.add_argument("lam", type=float, help="Poisson rate parameter λ (> 0)")
    parser.add_argument("n", type=int, help="Number of points to calculate (prints rows for i=0..n-1)")
    return parser.parse_args(argv)


def main(argv: List[str]) -> int:
    args = parse_args(argv)
    try:
        rows = generate_poisson_table(args.lam, args.n)
    except ValueError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2

    print_table(rows)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))


