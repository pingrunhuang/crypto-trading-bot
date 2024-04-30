import pandas as pd
import numpy as np


def vwap(df: pd.DataFrame) -> pd.DataFrame:
    def pv(high: float, low: float, close: float, vol: float):
        return (high + low + close) * vol / 3

    pv_func = np.vectorize(pv)
    df["pv"] = pv_func(
        df["high"].values, df["low"].values, df["close"].values, df["volume"].values
    )
    df["cum_pv"] = df["pv"].cumsum()
    df["cum_vol"] = df["volume"].cumsum()
    df["vwap"] = np.divide(df["cum_pv"], df["cum_vol"])
    return df
