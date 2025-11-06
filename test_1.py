import requests
import pandas as pd

url = "https://register.fiskeridir.no/uttrekk/fangstdata_2024.csv.zip"
print("Download:", url)

r = requests.get(url, timeout=60)
r.raise_for_status()
print("Size:", len(r.content)/1024/1024, "mb")

# read first 10 rows from the zip file
import io, zipfile
zf = zipfile.ZipFile(io.BytesIO(r.content))
name = [n for n in zf.namelist() if n.endswith(".csv")][0]

with zf.open(name) as f:
    df = pd.read_csv(f, sep=";", encoding="utf-8", nrows=10)

print("read 10 rows:")
print(df.head(3))
