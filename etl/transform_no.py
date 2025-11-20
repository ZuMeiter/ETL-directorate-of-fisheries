from .common import smart_read_csv, normalize, add_period, finish_monthly

def transform_landings_no(path: str):
    df = smart_read_csv(path)
    df = normalize(df)
    for c in ["dokumenttype (kode)","dokumenttype","dokument_type","dokument type"]:
        if c in df.columns:
            df = df[df[c].astype(str).str.strip() == "0"]  # sluttseddel
            break
    df = add_period(df)
    return finish_monthly(
        df,
        qty_cols=["rundvekt","kvantum_rundvekt","kvantum (rundvekt)"],
        val_cols=["forstehandsverdi","førstehandsverdi","forstehands_verdi",
                  "forstehandsverdi (nok)","førstehandsverdi (nok)"]
    )
