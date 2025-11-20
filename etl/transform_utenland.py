from .common import smart_read_csv, normalize, add_period, finish_monthly

def transform_landings_utenland(path: str):
    df = smart_read_csv(path)
    df = normalize(df)
    df = add_period(df)
    return finish_monthly(
        df,
        qty_cols=["rundvekt","kvantum_rundvekt","kvantum (rundvekt)","qty_kg","quantity_kg"],
        val_cols=["forstehandsverdi","førstehandsverdi","value_nok","price_nok"]
    )
