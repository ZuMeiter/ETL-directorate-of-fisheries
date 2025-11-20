from pathlib import Path
from etl.transform_no import transform_landings_no
from etl.transform_foreign import transform_landings_foreign

ROOT = Path(__file__).resolve().parents[1]
RAW = ROOT / "data" / "raw"
OUT = ROOT / "data" / "processed"
OUT.mkdir(parents=True, exist_ok=True)

def main():
    no_csv = RAW / "landings_no.csv"
    fr_csv = RAW / "landings_foreign.csv"

    print(f"[NO] → {no_csv}")
    no_monthly = transform_landings_no(str(no_csv))
    no_monthly.to_parquet(OUT/"landings_no_monthly.parquet", index=False)
    no_monthly.to_csv(OUT/"landings_no_monthly.csv", index=False)

    print(f"[FOREIGN] → {fr_csv}")
    fr_monthly = transform_landings_foreign(str(fr_csv))
    fr_monthly.to_parquet(OUT/"landings_foreign_monthly.parquet", index=False)
    fr_monthly.to_csv(OUT/"landings_foreign_monthly.csv", index=False)

    print("✅ Готово: data/processed/*.csv|*.parquet")

if __name__ == "__main__":
    main()
