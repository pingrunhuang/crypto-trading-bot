from typing import Optional
import pandas as pd
import numpy as np
from datetime import datetime
import requests
import zipfile
from io import BytesIO
import logging
from mongo_utils import MongoManger
import os


logger = logging.getLogger(__name__)


class BNCConn:
    def __init__(self) -> None:
        self.db_manager = MongoManger("history")

    def store_spot_trades(self, sym_base:str, sym_quote:str, dt:Optional[datetime]=None):
        if not dt:
            dt = datetime.utcnow()
        sym_root = f"{sym_base.upper()}{sym_quote.upper()}"
        endpoint = f"https://data.binance.vision/data/spot/daily/trades/{sym_root}"
        dt_str = dt.strftime("%Y-%m-%d")
        filename = f"{sym_root}-trades-{dt_str}"

        if not os.path.exists(filename):
            endpoint = f"{endpoint}/{filename}.zip"
            ret = requests.get(endpoint, stream=True)
            if ret.ok:
                logger.info(endpoint)
                z = zipfile.ZipFile(BytesIO(ret.content))
                z.extractall(filename)
            else:
                logger.error(ret.reason)
                if ret.status_code == 404:
                    return
                else:
                    ret.raise_for_status()
        df = pd.read_csv(
            f"{filename}/{filename}.csv",
            names=["trade_id", "price", "qty", "quo_qty", "datetime", "isBuyerMaker", "isMatch"]
        )
        df["buy_sell"] = np.where(df["isBuyerMaker"], "sell", "buy")
        df["datetime"] = pd.to_datetime(df["datetime"],unit="ms")
        df["symbol"] = f"{sym_base}{sym_quote}.BNC"
        df["symbol_base"] = sym_base
        df["symbol_quote"] = sym_quote
        df["exchange"] = "BNC"
        df.drop(["isBuyerMaker", "isMatch"], axis=1, inplace=True)
        logger.debug(df.head())
        self.db_manager.batch_upsert(df.to_dict("records"), "trades", "trade_id")
