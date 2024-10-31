import pandas as pd
from mongo_utils import MongoManager
import json
import logging
import datetime

logger = logging.getLogger(__name__)


def funds2mongo(funds_json: str, db_name: str = "fund"):
    with open(funds_json, encoding="utf-8") as f:
        src = json.load(f)
    raw_data = src["Data"]
    df = pd.DataFrame(raw_data)
    column_mapping = {
        "Fshye": "latest_balance",
        "Qrxx": "status",
        "Ywmc": "direction",
        "Zjzh": "trading_account",
        "Zzje": "funding_cny",
    }
    df.rename(column_mapping, inplace=True, axis=1)
    df["datetime"] = pd.to_datetime(df["Zzrq"] + df["Zzsj"], format="%Y-%m-%d%H:%M:%S")
    df["updated_at"] = pd.to_datetime(datetime.datetime.utcnow())
    numeric_cols = ["funding_cny", "latest_balance"]
    df[numeric_cols] = df[numeric_cols].astype(float)
    columns: list = list(column_mapping.values()) + ["datetime"]
    logger.info(f"Origianl number of transfer: {len(df)}")
    data = df[columns].drop_duplicates()
    logger.info(f"Unique number of transfer: {len(data)}")
    mgo_manager = MongoManager(db_name)
    mgo_manager.batch_insert(data.to_dict("records"), "account_balance")


if __name__ == "__main__":
    import os

    path = os.path.abspath(__file__)
    cur_dir = os.path.dirname(path)
    funds2mongo(os.path.join(cur_dir, "data/funding.json"))
