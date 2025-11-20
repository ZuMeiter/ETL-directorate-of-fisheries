import pandas as pd

def smart_read_csv(path: str) -> pd.DataFrame:
    encodings = ["utf-8-sig", "utf-16", "latin1"]
    seps = [";", ",", "\t"]

    last_err = None

    for enc in encodings:
        for sep in seps:
            try:
                print(f"[DEBUG] try read {path} enc={enc} sep={repr(sep)}")
                df = pd.read_csv(path, sep=sep, encoding=enc, low_memory=False)

                # 🔍 Хак: якщо колонка одна і в її назві є ; або таби – значить, парсинг «кривий»
                if len(df.columns) == 1:
                    header = str(df.columns[0])
                    if "\t" in header or ";" in header or "," in header:
                        print(f"[DEBUG] skip combo enc={enc} sep={repr(sep)} – header looks unsplit:", header)
                        continue

                return df

            except (UnicodeDecodeError, pd.errors.ParserError) as e:
                last_err = e
                continue

    # Фолбек – хай pandas сам підбере роздільник
    print("[WARN] fallback to auto sep detection")
    if last_err:
        print("[WARN] last error:", last_err)
    return pd.read_csv(path, sep=None, engine="python", low_memory=False)




def to_float(x) -> float:
    """Нормализует числовые строки из Fiskeridir в float."""
    if pd.isna(x):
        return 0.0

    s = str(x).strip()

    # пусто или просто минус -> считаем 0
    if not s or s in {"-", "."}:
        return 0.0

    # убираем все странные разделители
    for ch in ["\xa0", " ", "\n", "\r", "\t"]:
        s = s.replace(ch, "")

    # норвежские запятые в точку
    s = s.replace(",", ".")

    try:
        return float(s)
    except ValueError:
        # здесь можно либо вернуть 0.0, либо явно упасть с понятным текстом
        raise ValueError(f"Не можу перетворити значення '{x}' у число")


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

def finish_monthly(df: pd.DataFrame, qty_cols=None, val_cols=None) -> pd.DataFrame:
    """
    Перетворює wide-таблицю з Fiskeridir виду:
    [<місяць>, 2022, 2023, 2024, 2025, ...]
    у long-формат з колонками:
    year, month, rundvekt_tonn, forstehandsverdi_nok, rows.
    """
    df = df.copy()

    # 1. Колонка з назвою місяця (перша нецифрова)
    non_numeric = [c for c in df.columns if not str(c).isdigit()]
    if not non_numeric:
        raise ValueError("Не знайшов колонку з місяцем.")
    month_col = non_numeric[0]
    df = df.rename(columns={month_col: "month_name"})

    # 2. Річні колонки (2022, 2023, 2024, 2025, ...)
    year_cols = [c for c in df.columns if str(c).isdigit()]
    if not year_cols:
        raise ValueError("Не знайшов річні колонки (2022, 2023, ...).")

    # 3. wide -> long
    long = df.melt(
        id_vars=["month_name"],
        value_vars=year_cols,
        var_name="year",
        value_name="rundvekt_tonn",
    )

    # 4. Чистимо й приводимо типи
    long["year"] = long["year"].astype(int)
    long["month_name"] = long["month_name"].astype(str).str.strip()

    # прибираємо службові рядки, які залізли як дані
    long = long[~long["month_name"].str.contains("måned", case=False, na=False)]
    long = long[
        ~long["rundvekt_tonn"]
        .astype(str)
        .str.contains("rundvekt", case=False, na=False)
    ]

    # 5. мапа місяців норвезькою -> номер місяця
    month_map = {
        "januar": 1,
        "februar": 2,
        "mars": 3,
        "april": 4,
        "mai": 5,
        "juni": 6,
        "juli": 7,
        "august": 8,
        "september": 9,
        "oktober": 10,
        "november": 11,
        "desember": 12,
    }
    long["month"] = long["month_name"].str.lower().map(month_map)

    # 6. Кількість у тоннах
    long["rundvekt_tonn"] = long["rundvekt_tonn"].map(to_float)

    # 7. Додаткові поля під контракт
    long["forstehandsverdi_nok"] = 0.0
    long["rows"] = 1

    # 8. Фінальний датафрейм
    long = long.dropna(subset=["month"])

    return (
        long[["year", "month", "rundvekt_tonn", "forstehandsverdi_nok", "rows"]]
        .sort_values(["year", "month"])
        .reset_index(drop=True)
    )
