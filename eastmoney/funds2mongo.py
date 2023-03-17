import logging
import pandas as pd
from mongo_utils import MongoManger
import json

logging.basicConfig(level=logging.DEBUG, format="%(asctime)s|%(levelname)s|%(message)s")

logger = logging.getLogger(__name__)


def funds2mongo(funds_json:str):
    with open(funds_json, encoding="utf-8") as f:
        src = json.load(f)
    raw_data = src["Data"]
    df = pd.DataFrame(raw_data)
    column_mapping = {
        "Fshye": "latest_balance",
        "Qrxx": "status",
        "Ywmc": "direction",
        "Zjzh": "trading_account",
        "Zzje": "funding_cny"
    }
    df.rename(column_mapping, inplace=True, axis=1)
    df["datetime"] = pd.to_datetime(df["Zzrq"] + df["Zzsj"], format="%Y-%m-%d%H:%M:%S")
    numeric_cols = ["funding_cny", "latest_balance"]
    df[numeric_cols] = df[numeric_cols].astype(float)
    columns:list = list(column_mapping.values()) + ["datetime"]
    logger.info(f"Origianl number of transfer: {len(df)}")
    data = df[columns].drop_duplicates()
    logger.info(f"Unique number of transfer: {len(data)}")
    mgo_manager = MongoManger()
    clc = mgo_manager.get_database().get_collection("account_balance")
    clc.insert_many(data.to_dict("records"))


if __name__ == "__main__":
    import os
    path = os.path.abspath(__file__)
    cur_dir = os.path.dirname(path)
    funds2mongo(os.path.join(cur_dir, "data/funds.json"))