"""Plot landings/imports CSV as stacked monthly bars.

This script is tailored for CSVs exported like the attached file where the first
row contains years and the second row contains column names (a two-line header).

Usage example:
        python plot_landings.py "path/to/landings.csv" --year 2025 --top 8 --output landings_2025.png

Options:
    --main: filter to a top-level group like 'Pelagisk fisk' or 'Torsk og torskeartet fisk'
    --top: show top N Art - gruppe by total tonnage and lump the rest as 'Other'
"""
from __future__ import annotations

import argparse
from collections import defaultdict
from typing import List, Optional

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


MONTH_ORDER = [
    "Januar",
    "Februar",
    "Mars",
    "April",
    "Mai",
    "Juni",
    "Juli",
    "August",
    "September",
    "Oktober",
    "November",
    "Desember",
    "Levert påfølgende år",
    "Totalt",
]


def _clean_number(x):
    if pd.isna(x):
        return np.nan
    s = str(x).strip()
    if s == "":
        return np.nan
    # Remove non-breaking spaces and common thousands separators (space, narrow no-break)
    for ch in ["\u00A0", "\u202F", " "]:
        s = s.replace(ch, "")
    # Replace commas used as decimals with dot (if any)
    s = s.replace(',', '.')
    # If after cleaning it's still not numeric, return NaN
    try:
        return float(s)
    except ValueError:
        return np.nan


def read_landings(csv_path: str) -> pd.DataFrame:
    """Read the two-row-header CSV and return a cleaned DataFrame.

    The function handles the case where the first row contains years and the second
    row contains the column names (so we use header=[0,1] and collapse appropriately).
    """
    # Try a few encodings because the file may be UTF-16 or contain Windows encodings
    encodings = ['utf-8-sig', 'utf-16', 'cp1252', 'latin1']
    last_err = None
    for enc in encodings:
        try:
            df = pd.read_csv(csv_path, sep='\t', header=[0, 1], engine='python', encoding=enc)
            last_err = None
            break
        except Exception as e:
            last_err = e
            df = None
    if df is None:
        raise last_err

    # Flatten multiindex columns into a single-level schema.
    new_cols: List[str] = []
    for top, bottom in df.columns:
        top = '' if pd.isna(top) else str(top).strip()
        bottom = '' if pd.isna(bottom) else str(bottom).strip()
        if bottom == 'Rundvekt (tonn)':
            # top contains the year (e.g., '2022')
            colname = top
        else:
            colname = bottom
        new_cols.append(colname)

    df.columns = new_cols

    # Expected id columns
    id_cols = ['Landingsmåned', 'Art - hovedgruppe', 'Art - gruppe']
    # Some files may have leading/trailing whitespace in month names
    df[id_cols[0]] = df[id_cols[0]].astype(str).str.strip()

    # Identify year columns (columns that look like year numbers)
    year_cols = [c for c in df.columns if c.isdigit() or (isinstance(c, str) and c.strip().isdigit())]

    # Clean numeric values in year columns
    for c in year_cols:
        df[c] = df[c].apply(_clean_number)

    # Melt into long format
    melted = df.melt(id_vars=id_cols, value_vars=year_cols, var_name='Year', value_name='Tonn')

    # Drop NaN and zero values for plotting convenience
    melted['Tonn'] = melted['Tonn'].fillna(0.0)

    return melted


def plot_stacked_monthly(
    df_long: pd.DataFrame,
    year: int,
    main_group: Optional[str] = None,
    top_n: int = 8,
    title: Optional[str] = None,
    figsize=(12, 7),
    output: Optional[str] = None,
):
    """Create a stacked bar chart of monthly tonnage for the given year.

    - Filters by `main_group` if provided.
    - Keeps top_n groups by total tonnage and lumps the rest into 'Other'.
    """
    df = df_long.copy()
    df = df[df['Year'].astype(int) == int(year)]

    if main_group:
        df = df[df['Art - hovedgruppe'] == main_group]

    # Aggregate tonnage by month and Art - gruppe
    agg = df.groupby(['Landingsmåned', 'Art - gruppe'], as_index=False)['Tonn'].sum()

    # Determine top N groups by total tonnage over the year
    totals = agg.groupby('Art - gruppe', as_index=False)['Tonn'].sum().sort_values('Tonn', ascending=False)
    top_groups = totals['Art - gruppe'].iloc[:top_n].tolist()

    # Map small groups to 'Other'
    agg['Group'] = agg['Art - gruppe'].where(agg['Art - gruppe'].isin(top_groups), 'Other')

    pivot = agg.groupby(['Landingsmåned', 'Group'], as_index=False)['Tonn'].sum().pivot(index='Landingsmåned', columns='Group', values='Tonn').fillna(0)

    # Ensure month order
    # Keep only months present in data but order them according to MONTH_ORDER
    months_present = [m for m in MONTH_ORDER if m in pivot.index]
    # If some months are present but not in MONTH_ORDER, append them at end in natural order
    for m in pivot.index:
        if m not in months_present:
            months_present.append(m)

    pivot = pivot.reindex(months_present).fillna(0)

    # Plot stacked bars
    fig, ax = plt.subplots(figsize=figsize)
    cols = pivot.columns.tolist()
    # Choose colormap
    cmap = plt.get_cmap('tab20')
    colors = [cmap(i % 20) for i in range(len(cols))]

    bottom = np.zeros(len(pivot))
    x = np.arange(len(pivot))
    for i, col in enumerate(cols):
        vals = pivot[col].values
        ax.bar(x, vals, bottom=bottom, label=col, color=colors[i])
        bottom += vals

    ax.set_xticks(x)
    ax.set_xticklabels(pivot.index, rotation=45, ha='right')
    ax.set_ylabel('Rundvekt (tonn)')
    ax.set_title(title or f'Landingsmåned - {year}' + (f' ({main_group})' if main_group else ''))
    ax.legend(title='Art - gruppe', bbox_to_anchor=(1.02, 1), loc='upper left')
    plt.tight_layout()

    if output:
        fig.savefig(output, dpi=150)

    return fig


def _cli():
    parser = argparse.ArgumentParser(description='Plot stacked monthly landings from CSV')
    parser.add_argument('csv', help='Path to landings CSV')
    parser.add_argument('--year', type=int, required=True, help='Year to plot (e.g., 2025)')
    parser.add_argument('--main', help="Filter to a top-level group like 'Pelagisk fisk' (optional)")
    parser.add_argument('--top', type=int, default=8, help='Top N Art - gruppe to show; rest lumped to Other')
    parser.add_argument('--output', help='Output image path (e.g., landings_2025.png)')
    args = parser.parse_args()

    df_long = read_landings(args.csv)
    plot_stacked_monthly(df_long, year=args.year, main_group=args.main, top_n=args.top, output=args.output)


if __name__ == '__main__':
    _cli()
