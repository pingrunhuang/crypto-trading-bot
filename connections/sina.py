import pandas as pd
import datetime
import json
import requests

"""
bj
sh000001
sz
xcode= code.replace('.XSHG','').replace('.XSHE','')                      #证券代码编码兼容处理 
xcode='sh'+xcode if ('XSHG' in code)  else  'sz'+xcode  if ('XSHE' in code)  else code

"""


# sina新浪全周期获取函数，分钟线 5m,15m,30m,60m  日线1d=240m   周线1w=1200m  1月=7200m
def get_price_sina(code, end_date="", count=10, frequency="60m"):  # 新浪全周期获取函数
    frequency = (
        frequency.replace("1d", "240m").replace("1w", "1200m").replace("1M", "7200m")
    )
    mcount = count
    ts = int(frequency[:-1]) if frequency[:-1].isdigit() else 1  # 解析K线周期数
    if (end_date != "") & (frequency in ["240m", "1200m", "7200m"]):
        end_date = (
            pd.to_datetime(end_date)
            if not isinstance(end_date, datetime.datetime)
            else end_date
        )  # 转换成datetime
        unit = (
            4 if frequency == "1200m" else 29 if frequency == "7200m" else 1
        )  # 4,29多几个数据不影响速度
        count = (
            count + (datetime.datetime.now() - end_date).days // unit
        )  # 结束时间到今天有多少天自然日(肯定 >交易日)
        # print(code,end_date,count)
    URL = f"http://money.finance.sina.com.cn/quotes_service/api/json_v2.php/CN_MarketData.getKLineData?symbol={code}&scale={ts}&ma=5&datalen={count}"
    dstr = json.loads(requests.get(URL).content)
    # df=pd.DataFrame(dstr,columns=['day','open','high','low','close','volume'],dtype='float')
    df = pd.DataFrame(dstr, columns=["day", "open", "high", "low", "close", "volume"])
    df["open"] = df["open"].astype(float)
    df["high"] = df["high"].astype(float)
    # 转换数据类型
    df["low"] = df["low"].astype(float)
    df["close"] = df["close"].astype(float)
    df["volume"] = df["volume"].astype(float)
    df.day = pd.to_datetime(df.day)
    df.set_index(["day"], inplace=True)
    df.index.name = ""  # 处理索引
    if (end_date != "") & (frequency in ["240m", "1200m", "7200m"]):
        return df[df.index <= end_date][-mcount:]  # 日线带结束时间先返回
    return df
