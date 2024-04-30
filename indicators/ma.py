import pandas as pd


def sma(df: pd.DataFrame, windows: int, prx_col:str="close")->pd.DataFrame:
    """
    simple moving average
    """
    df["sma"] = df[prx_col].rolling(windows).mean()
    return df


def ema(df:pd.DataFrame, 
        windows:int, 
        prx_col:str="close", 
        smooth_factor:float=2)->pd.DataFrame:
    """
    formula: ema[i] = alpha*pi + (1-alpha)*ema[i-1]
    df: dataframe that contain price data
    windows: time window for ema calculation, eg. days, hours, minutes
    prx_col: price colume in dataframe
    smooth_factor: 
    """
    alpha = smooth_factor/(windows+1)
    df["ema"] = df[prx_col].ewm(span=windows, alpha=alpha, adjust=False).mean()
    return df


def macd(df:pd.DataFrame):
    pass


