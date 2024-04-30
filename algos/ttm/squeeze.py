"""
Created on 1/18/2021

@author: frank
"""

import pandas as pd
import plotly.graph_objects as go
from trading_strategies.snapshot import fetch_mongo_df
from tools.db_infos import *


symbols = ["BTCUSDT.BNC", "ETHUSDT.BNC", "EOSUSDT.BNC", "LTCUSDT.BNC", "XRPUSDT.BNC"]
# symbols = ["BTCUSDT.BNC"]
windows = 20
KELTNER_CHANEL_MULTIPLIER = 1.5


def in_squeeze(x):
    return x["lower_band"] > x["lower_keltner"] and x["upper_band"] < x["upper_keltner"]


for sym in symbols:
    df = fetch_mongo_df(loc1_read_url, "bar_1d", sym, "2020-10-01", "2021-01-01")
    df[["close", "open", "high", "low", "volume"]] = df[
        ["close", "open", "high", "low", "volume"]
    ].applymap(lambda x: float(x.to_decimal()))
    df[f"{windows}smac"] = df["close"].rolling(window=windows).mean()
    df[f"{windows}std"] = df["close"].rolling(window=windows).std()
    df["upper_band"] = df[f"{windows}smac"] + 2 * df[f"{windows}std"]
    df["lower_band"] = df[f"{windows}smac"] - 2 * df[f"{windows}std"]
    candle = go.Candlestick(
        x=df["datetime"],
        open=df["open"],
        high=df["high"],
        low=df["low"],
        close=df["close"],
    )
    upper_band = go.Scatter(x=df["datetime"], y=df["upper_band"], name="upper")
    lower_band = go.Scatter(x=df["datetime"], y=df["lower_band"], name="lower")
    df["true_range"] = df["high"] - df["close"]
    df["average_true_range"] = df["true_range"].rolling(window=windows).mean()
    df["upper_keltner"] = df[f"{windows}smac"] + (
        df["average_true_range"] * KELTNER_CHANEL_MULTIPLIER
    )
    df["lower_keltner"] = df[f"{windows}smac"] - (
        df["average_true_range"] * KELTNER_CHANEL_MULTIPLIER
    )
    df["squeeze_on"] = df.apply(in_squeeze, axis=1)
    # upper_kc = go.Scatter(x=df["datetime"], y=df["upper_KC"], name="upper_kc")
    # lower_kc = go.Scatter(x=df["datetime"], y=df["lower_KC"], name="lower_kc")
    # fig = go.Figuuhn5re(data=[candle, upper_band, lower_band, upper_kc, lower_kc])
    # fig.show()
    if df.iloc[-3]["squeeze_on"] and df.iloc[-1]["squeeze_on"]:
        print(f"{sym} is coming out of squeeze")
