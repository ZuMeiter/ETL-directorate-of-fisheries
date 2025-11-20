import pandas as pd

def smart_read_csv(path: str) -> pd.DataFrame:
    try:
        return pd.read_csv(path, sep=";", encoding="utf-8-sig", low_memory=False)
    except Exception:
        return pd.read_csv(path, sep=",", encoding="utf-8-sig", low_memory=False)

def to_float(x):
    if pd.isna(x): return 0.0
    return float(str(x).replace(" ", "").replace(",", "."))

def normalize(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = [c.strip().lower() for c in df.columns]
    return df

def add_period(df: pd.DataFrame) -> pd.DataFrame:
    for c in ["date","landingsdato","landings_dato","landingsdato (yyyy-mm-dd)"]:
        if c in df.columns:
            df["date"] = pd.to_datetime(df[c], errors="coerce", utc=True)
            df = df.dropna(subset=["date"])
            df["year"] = df["date"].dt.year
            df["month"] = df["date"].dt.month
            break
    return df

def finish_monthly(df: pd.DataFrame, qty_cols, val_cols=None) -> pd.DataFrame:
    qty = next((c for c in qty_cols if c in df.columns), None)
    if not qty:
        raise ValueError("Не знайшов колонку з кількістю (kg).")
    df["rundvekt_kg"]   = df[qty].map(to_float)
    df["rundvekt_tonn"] = df["rundvekt_kg"] / 1000.0

    val = next((c for c in (val_cols or []) if c in df.columns), None)
    df["forstehandsverdi_nok"] = df[val].map(to_float) if val else 0.0

    return (df.groupby(["year","month"], as_index=False)
              .agg(rundvekt_tonn=("rundvekt_tonn","sum"),
                   forstehandsverdi_nok=("forstehandsverdi_nok","sum"),
                   rows=("rundvekt_kg","count"))
              .sort_values(["year","month"]))
