import argparse
import io
import zipfile
from typing import Optional

import pandas as pd
import requests


def to_float(x):
    if pd.isna(x):
        return 0.0
    return float(str(x).replace(" ", "").replace("\u00A0", "").replace(",", "."))


MONTH_MAP = {
    "Januar": 1,
    "Februar": 2,
    "Mars": 3,
    "April": 4,
    "Mai": 5,
    "Juni": 6,
    "Juli": 7,
    "August": 8,
    "September": 9,
    "Oktober": 10,
    "November": 11,
    "Desember": 12,
}


def process_remote_year(year: int) -> pd.DataFrame:
    """Original behaviour: download zipped CSV for a single year and aggregate monthly."""
    url = f"https://register.fiskeridir.no/uttrekk/fangstdata_{year}.csv.zip"
    print(f"Download data - {year}...")
    r = requests.get(url, timeout=120)
    r.raise_for_status()

    zf = zipfile.ZipFile(io.BytesIO(r.content))
    csv_name = [n for n in zf.namelist() if n.lower().endswith(".csv")][0]

    with zf.open(csv_name) as f:
        df = pd.read_csv(f, sep=";", encoding="utf-8", low_memory=False)

    df.columns = [c.strip().lower() for c in df.columns]

    # nøkel kolonner
    date_col = next(c for c in ["landingsdato", "landings_dato", "landingsdato (yyyy-mm-dd)"] if c in df.columns)
    dok_col = next(c for c in ["dokumenttype (kode)", "dokumenttype", "dokument_type"] if c in df.columns)
    qty_col = next(c for c in ["rundvekt", "kvantum_rundvekt", "kvantum (rundvekt)"] if c in df.columns)
    val_col = next((c for c in ["forstehandsverdi", "førstehandsverdi", "forstehands_verdi"] if c in df.columns), None)

    # sluttseddel
    df = df[df[dok_col].astype(str).str.strip() == "0"]

    # data
    df["date"] = pd.to_datetime(df[date_col], errors="coerce", utc=True)
    df = df.dropna(subset=["date"])
    df["year"] = df["date"].dt.year
    df["month"] = df["date"].dt.month

    df["rundvekt_tonn"] = df[qty_col].map(to_float)
    if val_col:
        df["forstehandsverdi_nok"] = df[val_col].map(to_float)
    else:
        df["forstehandsverdi_nok"] = 0.0

    monthly = (
        df.groupby(["year", "month"], as_index=False)
        .agg(rundvekt_tonn=("rundvekt_tonn", "sum"), forstehandsverdi_nok=("forstehandsverdi_nok", "sum"), rows=("rundvekt_tonn", "count"))
        .sort_values(["year", "month"])
    )
    return monthly


def process_local_landings(csv_path: str, output_dir: Optional[str] = None) -> pd.DataFrame:
    """Process the provided local landings CSV (two-line header style) and return monthly aggregated DataFrame.

    The input format is expected to contain columns: 'Landingsmåned', 'Art - hovedgruppe', 'Art - gruppe', and year columns like '2022', '2023', ... with 'Rundvekt (tonn)'.
    """
    # Try multiple encodings
    encodings = ["utf-8-sig", "utf-16", "cp1252", "latin1"]
    df = None
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

    # Flatten columns
    new_cols = []
    for top, bottom in df.columns:
        top = '' if pd.isna(top) else str(top).strip()
        bottom = '' if pd.isna(bottom) else str(bottom).strip()
        if bottom == 'Rundvekt (tonn)':
            colname = top
        else:
            colname = bottom
        new_cols.append(colname)
    df.columns = new_cols

    id_col = 'Landingsmåned'
    df[id_col] = df[id_col].astype(str).str.strip()

    # Identify year columns
    year_cols = [c for c in df.columns if isinstance(c, str) and c.strip().isdigit()]

    # Clean numeric values
    for c in year_cols:
        df[c] = df[c].apply(lambda x: float(str(x).strip().replace('\u00A0', '').replace(' ', '').replace(',', '.')) if pd.notna(x) and str(x).strip() != '' else 0.0)

    # Melt
    melted = df.melt(id_vars=[id_col], value_vars=year_cols, var_name='Year', value_name='Tonn')

    # Map month names to numbers; drop rows with unknown months
    melted['MonthName'] = melted[id_col].str.strip()
    melted['month'] = melted['MonthName'].map(MONTH_MAP)
    melted = melted.dropna(subset=['month'])
    melted['year'] = melted['Year'].astype(int)

    # Aggregate
    monthly = (
        melted.groupby(['year', 'month'], as_index=False)['Tonn']
        .sum()
        .rename(columns={'Tonn': 'rundvekt_tonn'})
        .assign(forstehandsverdi_nok=0.0)
    )

    # Add a rows count column (approx)
    sizes = melted.groupby(['year', 'month'], as_index=False).size().reset_index()
    # sizes will have a column with default name 0 or 'size'; normalize it to 'rows'
    if sizes.shape[1] == 3:
        # columns: year, month, 0
        sizes.columns = ['year', 'month', 'rows']
    else:
        # Fallback: try to find the last column and rename it
        cols = list(sizes.columns)
        cols[-1] = 'rows'
        sizes.columns = cols
    rows = sizes
    monthly = monthly.merge(rows, on=['year', 'month'], how='left')

    # Save per year as parquet files similar to original behaviour
    for y in monthly['year'].unique():
        out_df = monthly[monthly['year'] == y].sort_values('month')
        out_path = f"monthly_{y}.parquet"
        try:
            out_df.to_parquet(out_path, index=False)
            print(f"Saved: {out_path}")
        except Exception:
            # parquet engine not available — fall back to CSV
            csv_path = f"monthly_{y}.csv"
            out_df.to_csv(csv_path, index=False)
            print(f"Parquet engine missing. Saved CSV instead: {csv_path}")

    return monthly


def _cli():
    parser = argparse.ArgumentParser(description='Produce monthly aggregates from remote API or a local landings CSV')
    parser.add_argument('--year', type=int, help='Year to download/process for remote mode')
    parser.add_argument('--input', help='Local landings CSV path (use this instead of remote download)')
    args = parser.parse_args()

    if args.input:
        print(f"Processing local CSV: {args.input}")
        monthly = process_local_landings(args.input)
        print(monthly.head(12))
    elif args.year:
        monthly = process_remote_year(args.year)
        print(monthly.head(12))
        try:
            monthly.to_parquet(f"monthly_{args.year}.parquet", index=False)
            print(f"File saved: monthly_{args.year}.parquet")
        except Exception:
            csv_path = f"monthly_{args.year}.csv"
            monthly.to_csv(csv_path, index=False)
            print(f"Parquet engine missing. Saved CSV instead: {csv_path}")
    else:
        parser.error('Either --input or --year must be provided')


if __name__ == '__main__':
    _cli()
