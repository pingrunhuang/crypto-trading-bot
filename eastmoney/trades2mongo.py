import logging
import pandas as pd
from eastmoney.mongo_utils import MongoManger
import json

logging.basicConfig(level=logging.DEBUG, format="%(asctime)s|%(levelname)s|%(message)s")

logger = logging.getLogger(__name__)

def trades2mongo(historical_json:str, db_name:str="fund"):
    
    with open(historical_json, encoding="utf-8") as fp:
        historical = json.load(fp)
    df = pd.DataFrame(historical)
    column_mapping = {
        "Cjbh": "trade_id",
        "Zqdm": "symbol_id",
        "Zqmc": "symbol",
        "Cjjg": "trade_prx",
        "Cjsl": "trade_amt",
        "Cjje": "trade_cny",
        "Sxf": "trade_fee",
        "Yhs": "stamp_tax",
        "Ghf": "transmit_fee",
        "Jygf": "regulate_fee",
        "Gfye": "position",
        "Zjye": "balance",
        "Mmlb": "buysell"
    }
    
    df.rename(column_mapping, inplace=True, axis=1)
    numeric_cols = [
        "trade_prx", "trade_amt", "trade_fee", "stamp_tax", "transmit_fee", "regulate_fee", "position", "balance"
    ]
    df[numeric_cols] = df[numeric_cols].astype(float)
    df["datetime"] = pd.to_datetime(df["Cjrq"] + df["Cjsj"], format="%Y%m%d%H%M%S")
    columns:list = list(column_mapping.values()) + ["datetime"]
    logger.info(f"Origianl number of trades: {len(df)}")
    data = df[columns].drop_duplicates()
    logger.info(f"Unique number of trades: {len(data)}")
    mgo_manager = MongoManger(db_name)
    mgo_manager.batch_upsert(data.to_dict("records"), "em_trades", "trade_id")


if __name__=="__main__":
    
    trades2mongo("data/trades.json")