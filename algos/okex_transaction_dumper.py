import pandas as pd
from utils import safe_decimal128
from pymongo import MongoClient, UpdateOne
import yaml
import logging

logger = logging.getLogger(__name__)

df = pd.read_csv("TradeAccount-History-202109180015.csv")
df["order_id"] = df["order_id"].astype(str)
df["datetime"] = df["datetime"].apply(pd.to_datetime)
decimal_cols = [
    "amount",
    "position_balance",
    "balance",
    "balance_change",
    "position_change",
    "fee",
    "pl",
]
df[decimal_cols] = df[decimal_cols].applymap(safe_decimal128)
df["exchange"] = "OKX"

print(df.dtypes)

with open("configs/settings.yaml") as f:
    config = yaml.safe_load(f)

data = df.to_dict("records")
to_upsert = []
for entry in data:
    fil = {"order_id": entry["order_id"]}
    doc = {"$set": entry}
    to_upsert.append(UpdateOne(fil, doc, upsert=True))


with MongoClient(config["mgo_url"]) as cli:
    clc = cli.get_database("crypto").get_collection("transactions")
    res = clc.bulk_write(to_upsert)
    logger.info(f"Upserted {res.inserted_count}")
