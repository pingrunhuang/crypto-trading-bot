import logging
import logging
import pandas as pd
from mongo_utils import MongoManager, AsyncMongoManager
from connections.eastmoney.kline_grabber import fetch_kline_1d, fetch_kline_1h


logger = logging.getLogger(__name__)


def calc_total_position(data: list):
    total_buy = 0
    total_sell = 0
    position = {}
    for entry in data:
        if entry["Mmlb"] == "B":
            total_buy += float(entry["Cjje"])
            ntl = -float(entry["Cjje"])
        elif entry["Mmlb"] == "S":
            total_sell += float(entry["Cjje"])
            ntl = float(entry["Cjje"])
        else:
            ntl = 0
        position[entry["Zqmc"]] = float(position.get(entry["Zqmc"], 0)) + ntl

    logger.info(f"Total buy: {total_buy}")
    logger.info(f"Total sell: {total_sell}")
    logger.info(f"Position: {position}")
    logger.info(f"Total money spent: {total_buy-total_sell}")
    logger.info(f"Last datetime: {data[0]['Cjrq']}")
    return total_buy - total_sell


def calc_pnl(data: dict, assets_ntl: float):
    latest_assets_ntl = float(data["Data"][0]["Zzc"])
    logger.info(f"Total pnl: {latest_assets_ntl-assets_ntl}")


def calc_funding():
    manager = MongoManager("fund")
    clc = manager.db.get_collection("account_balance")
    res = clc.find({}, sort=[("datetime", -1)])
    df = pd.DataFrame(res)
    logger.info(f"Funding df: {df.head()}")
    withdraw_df = df[(df["funding_cny"] < 0) & (df["status"] == "交易成功")]
    total_withdrawal = abs(withdraw_df["funding_cny"].sum())
    logger.info(f"Total withdraw: {total_withdrawal}")
    deposit_df = df[(df["funding_cny"] > 0) & (df["status"] == "交易成功")]
    total_deposit = deposit_df["funding_cny"].sum()
    logger.info(f"Total deposite: {total_deposit}")
    logger.info(f"Latest balance: {total_deposit-total_withdrawal}")


async def fetch_kline(sec_id: str, freq: str, sdt: str, edt: str, db_name: str):
    db = AsyncMongoManager(db_name)
    if freq == "1d":
        res = await fetch_kline_1d(sec_id, sdt, edt)
        await db.batch_upsert(res, "bar_1d", keys=["datetime", "code"])
    elif freq == "1h":
        res = await fetch_kline_1h(sec_id, sdt, edt)
        await db.batch_upsert(res, "bar_1h", keys=["datetime", "code"])
