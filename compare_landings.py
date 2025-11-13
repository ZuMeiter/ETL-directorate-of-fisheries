"""Compare total 'rundvekt_tonn' for the latest year between two landings CSVs.

This script uses `process_local_landings` from `monthly.py` to parse the two-line-header
CSV format you provided. For each file it picks the latest year present, sums the
`rundvekt_tonn` for that year, and prints the absolute and percentage differences.

Usage:
    python compare_landings.py --file1 "path/to/foreign.csv" --file2 "path/to/local.csv"

Optional:
    --label1/--label2 to name the series in output; --year to force a specific year.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Optional

import pandas as pd

from monthly import process_local_landings


def summarize_latest(csv_path: str, forced_year: Optional[int] = None) -> dict:
    """Return a summary dict with latest year and total rundvekt_tonn for that year."""
    monthly = process_local_landings(csv_path)
    years = sorted(monthly['year'].unique())
    if len(years) == 0:
        raise RuntimeError(f"No years found in {csv_path}")
    year = int(forced_year) if forced_year is not None else int(max(years))
    total = float(monthly.loc[monthly['year'] == year, 'rundvekt_tonn'].sum())
    return {'path': str(csv_path), 'year': year, 'total_tonn': total}


def compare(a: dict, b: dict) -> dict:
    diff = a['total_tonn'] - b['total_tonn']
    pct_of_b = (diff / b['total_tonn'] * 100.0) if b['total_tonn'] != 0 else None
    pct_of_a = (diff / a['total_tonn'] * 100.0) if a['total_tonn'] != 0 else None
    return {
        'file1': a,
        'file2': b,
        'difference_tonn': diff,
        'abs_difference_tonn': abs(diff),
        'pct_difference_of_file2': pct_of_b,
        'pct_difference_of_file1': pct_of_a,
    }


def _cli():
    parser = argparse.ArgumentParser(description='Compare latest-year rundvekt_tonn between two landings CSVs')
    parser.add_argument('--file1', required=True, help='First CSV path (e.g. foreign imports)')
    parser.add_argument('--file2', required=True, help='Second CSV path (e.g. local imports)')
    parser.add_argument('--label1', help='Label for first file', default='File 1')
    parser.add_argument('--label2', help='Label for second file', default='File 2')
    parser.add_argument('--year', type=int, help='Force a specific year to compare (optional)')
    parser.add_argument('--out', help='Optional output JSON path to save results')
    args = parser.parse_args()

    p1 = Path(args.file1)
    p2 = Path(args.file2)
    if not p1.exists():
        raise FileNotFoundError(f"File not found: {p1}")
    if not p2.exists():
        raise FileNotFoundError(f"File not found: {p2}")

    print(f"Processing {p1}...")
    s1 = summarize_latest(str(p1), forced_year=args.year)
    s1['label'] = args.label1

    print(f"Processing {p2}...")
    s2 = summarize_latest(str(p2), forced_year=args.year)
    s2['label'] = args.label2

    result = compare(s1, s2)

    # Pretty print
    print('\nSummary:')
    print(f"{s1['label']} ({Path(s1['path']).name}) - Year {s1['year']}: {s1['total_tonn']:,} tonn")
    print(f"{s2['label']} ({Path(s2['path']).name}) - Year {s2['year']}: {s2['total_tonn']:,} tonn")
    print(f"Difference (file1 - file2): {result['difference_tonn']:,} tonn")
    print(f"Absolute difference: {result['abs_difference_tonn']:,} tonn")
    if result['pct_difference_of_file2'] is not None:
        print(f"Difference as % of {s2['label']}: {result['pct_difference_of_file2']:.2f}%")
    if result['pct_difference_of_file1'] is not None:
        print(f"Difference as % of {s1['label']}: {result['pct_difference_of_file1']:.2f}%")

    if args.out:
        with open(args.out, 'w', encoding='utf-8') as fh:
            json.dump(result, fh, ensure_ascii=False, indent=2)
        print(f"Saved JSON result to {args.out}")


if __name__ == '__main__':
    _cli()
