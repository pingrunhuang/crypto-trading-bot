import pandas as pd


def sharp_ratio(df:pd.DataFrame):
    """
    df should contain a return_rate column which is calculated by price change
    for ohlcv chart is (close-prev_close)/prev_close 
    """
    risk_free_rate = 0.05 # can be adjusted
    df["excess_return"] = df["return_rate"]/risk_free_rate
    mean_return = df["excess_return"].mean()
    std_return = df["excess_return"].std()
    return mean_return/std_return