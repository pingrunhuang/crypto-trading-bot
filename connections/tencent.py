import json, requests, datetime
import pandas as pd


# 腾讯日线
def get_price_day_tx(code, end_date="", count=10, frequency="1d"):  # 日线获取
    unit = (
        "week" if frequency in "1w" else "month" if frequency in "1M" else "day"
    )  # 判断日线，周线，月线
    if end_date:
        end_date = (
            end_date.strftime("%Y-%m-%d")
            if isinstance(end_date, datetime.date)
            else end_date.split(" ")[0]
        )
    end_date = (
        "" if end_date == datetime.datetime.now().strftime("%Y-%m-%d") else end_date
    )  # 如果日期今天就变成空
    URL = f"http://web.ifzq.gtimg.cn/appstock/app/fqkline/get?param={code},{unit},,{end_date},{count},qfq"
    st = json.loads(requests.get(URL).content)
    ms = "qfq" + unit
    stk = st["data"][code]
    buf = stk[ms] if ms in stk else stk[unit]  # 指数返回不是qfqday,是day
    df = pd.DataFrame(
        buf, columns=["time", "open", "close", "high", "low", "volume"], dtype="float"
    )
    df.time = pd.to_datetime(df.time)
    df.set_index(["time"], inplace=True)
    df.index.name = ""  # 处理索引
    return df


# 腾讯分钟线
def get_price_min_tx(code, end_date=None, count=10, frequency="1d"):  # 分钟线获取
    ts = int(frequency[:-1]) if frequency[:-1].isdigit() else 1  # 解析K线周期数
    if end_date:
        end_date = (
            end_date.strftime("%Y-%m-%d")
            if isinstance(end_date, datetime.date)
            else end_date.split(" ")[0]
        )
    URL = f"http://ifzq.gtimg.cn/appstock/app/kline/mkline?param={code},m{ts},,{count}"
    st = json.loads(requests.get(URL).content)
    buf = st["data"][code]["m" + str(ts)]
    df = pd.DataFrame(
        buf, columns=["time", "open", "close", "high", "low", "volume", "n1", "n2"]
    )
    df = df[["time", "open", "close", "high", "low", "volume"]]
    df[["open", "close", "high", "low", "volume"]] = df[
        ["open", "close", "high", "low", "volume"]
    ].astype("float")
    df.time = pd.to_datetime(df.time)
    df.set_index(["time"], inplace=True)
    df.index.name = ""  # 处理索引
    df["close"][-1] = float(st["data"][code]["qt"][code][3])  # 最新基金数据是3位的
    return df
