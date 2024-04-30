from typing import Optional
import aiohttp
import pandas as pd
import logging


logger = logging.getLogger(__name__)


def _decode(data: dict) -> list:
    data = data["data"]
    code = data["code"]
    name = data["name"]
    klines = data["klines"]
    result = []
    for kline in klines:
        logger.info(kline)
        line = kline.split(",")
        result.append(
            {
                "name": name,
                "code": code,
                "datetime": pd.to_datetime(line[0]),
                "open": float(line[1]),
                "close": float(line[2]),
                "high": float(line[3]),
                "low": float(line[4]),
                "volume_qty": float(line[5]),
                "volume_ntl": float(line[6]),
            }
        )
    return result


async def fetch_kline_1d(secid: str, sdt: Optional[str], edt: Optional[str]):
    if not sdt and not edt:
        sdt = "0"
        edt = "20500101"
    url = f"http://push2his.eastmoney.com/api/qt/stock/kline/get?fields1=f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f11,f12,f13&fields2=f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61&beg={sdt}&end={edt}&rtntype=6&secid=1.{secid}&klt=101&fqt=1"
    async with aiohttp.ClientSession() as sess:
        res = await sess.get(url)
        data = await res.json()
        return _decode(data)


async def fetch_kline_1h(secid: str, sdt: Optional[str], edt: Optional[str]):
    if not sdt and not edt:
        sdt = "0"
        edt = "20500101"
    url = f"http://push2his.eastmoney.com/api/qt/stock/kline/get?fields1=f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f11,f12,f13&fields2=f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61&beg={sdt}&end={edt}&&rtntype=6&secid=1.{secid}&klt=60&fqt=1"
    async with aiohttp.ClientSession() as sess:
        res = await sess.get(url)
        data = await res.json()
        return _decode(data)
