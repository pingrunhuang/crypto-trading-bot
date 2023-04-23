import logging
import pandas as pd
from eastmoney.mongo_utils import MongoManger

logging.basicConfig(level=logging.DEBUG,
                    format="%(asctime)s|%(levelname)s|%(message)s")

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
            total_sell += float(entry['Cjje'])
            ntl = float(entry["Cjje"])
        else:
            ntl = 0
        position[entry["Zqmc"]] = float(position.get(entry["Zqmc"], 0)) + ntl

    logger.info(f"Total buy: {total_buy}")
    logger.info(f"Total sell: {total_sell}")
    logger.info(f"Position: {position}")
    logger.info(f"Total money spent: {total_buy-total_sell}")
    logger.info(f"Last datetime: {data[0]['Cjrq']}")
    return total_buy-total_sell


def calc_pnl(data: dict, assets_ntl: float):
    latest_assets_ntl = float(data["Data"][0]["Zzc"])
    logger.info(f"Total pnl: {latest_assets_ntl-assets_ntl}")


def calc_funding():
    manager = MongoManger()
    clc = manager.get_database().get_collection("account_balance")
    res = clc.find({}, sort=[("datetime", -1)])
    df = pd.DataFrame(res)
    logger.info(f"Funding df: {df.head()}")
    withdraw_df = df[(df['funding_cny'] < 0) & (df['status'] == "交易成功")]
    logger.info(f"Total withdraw: {abs(withdraw_df['funding_cny'].sum())}")
    deposite_df = df[(df['funding_cny'] > 0) & (df['status'] == "交易成功")]
    logger.info(f"Total deposite: {deposite_df['funding_cny'].sum()}")


if __name__ == "__main__":
    calc_funding()
