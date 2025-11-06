import io, zipfile, requests, pandas as pd

def to_float(x):
    if pd.isna(x): return 0.0
    return float(str(x).replace(" ", "").replace(",", "."))

year = 2024
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
date_col = next(c for c in ["landingsdato","landings_dato","landingsdato (yyyy-mm-dd)"] if c in df.columns)
dok_col  = next(c for c in ["dokumenttype","dokument_type"] if c in df.columns)
qty_col  = next(c for c in ["rundvekt","kvantum_rundvekt","kvantum (rundvekt)"] if c in df.columns)
val_col  = next((c for c in ["forstehandsverdi","førstehandsverdi","forstehands_verdi"] if c in df.columns), None)

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

monthly = df.groupby(["year","month"], as_index=False).agg(
    rundvekt_tonn=("rundvekt_tonn","sum"),
    forstehandsverdi_nok=("forstehandsverdi_nok","sum"),
    rows=("rundvekt_tonn","count")
).sort_values(["year","month"])

print("Finish:")
print(monthly.head(10))

# saved results
monthly.to_parquet(f"monthly_{year}.parquet", index=False)
print(f"File saved: monthly_{year}.parquet")
