"""Convert all monthly_*.parquet files in the repo root to CSV files.

Run:
    python convert_parquet_to_csv.py

This script writes files like `monthly_2025.csv` next to the parquet files.
"""
import glob
import sys
from pathlib import Path

import pandas as pd


def main():
    files = sorted(glob.glob('monthly_*.parquet'))
    if not files:
        print('No parquet files found matching monthly_*.parquet')
        return

    for f in files:
        p = Path(f)
        try:
            df = pd.read_parquet(f)
        except Exception as e:
            print(f'Failed to read {f}: {e}')
            continue
        out = p.with_suffix('.csv')
        df.to_csv(out, index=False)
        print(f'Saved CSV: {out}')


if __name__ == '__main__':
    main()
