import pandas as pd


def to_datetime_series(s, dayfirst=True):
    return pd.to_datetime(s, dayfirst=dayfirst, errors="coerce")

def to_str_id_like(x):
    if pd.isna(x):
        return None
    if isinstance(x, str):
        xs = x.strip()
        return xs if xs != "" else None
    try:
        f = float(x)
    except Exception:
        return str(x)
    if f.is_integer():
        return str(int(f))
    return str(x)
