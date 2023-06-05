from typing import Optional
import pandas as pd
import numpy as np
from datetime import datetime
import requests
from io import BytesIO
import logging
from mongo_utils import MongoManger, AsyncMongoManager
import shutil
import os
from utils import unzip
from contextlib import contextmanager
from consts import DATETIME, VOL, SYM, SYM_BASE, SYM_QUOTE, SYM_ROOT, EXCH, \
    BINANCE, BUY_SELL, AT, OPEN, HIGH, LOW, CLOSE, TICK, LOT
from connections.base import ABCConnection, ABCDownloader

logger = logging.getLogger(__name__)

class BNCDownloader(ABCDownloader):
    """
    binance historical data downloader
    """
    API_URL = "https://data.binance.vision/data"
    def __init__(self, db_name:str="history") -> None:
        super().__init__(db_name)
    
    @contextmanager
    def download_zip(self, endpoint:str, filename:str):
        if not os.path.exists(filename):
            endpoint = f"{endpoint}/{filename}.zip"
            ret = requests.get(endpoint, stream=True)
            logger.info(f"Dowloading zip file from: {endpoint}")
            unzip(filename, BytesIO(ret.content))
            if not ret.ok:
                logger.error(ret.reason)
                if ret.status_code != 404:
                    ret.raise_for_status()
        yield
        shutil.rmtree(filename)

    def fetch_spot_trades(self, 
                          sym_base:str, 
                          sym_quote:str, 
                          dt:Optional[datetime])->pd.DataFrame:
        if not dt:
            dt = datetime.utcnow()
        sym_root = f"{sym_base.upper()}{sym_quote.upper()}"
        endpoint = f"{self.API_URL}/spot/daily/trades/{sym_root}"
        dt_str = dt.strftime("%Y-%m-%d")
        filename = f"{sym_root}-trades-{dt_str}"
        with self.download_zip(endpoint, filename):
            df = pd.read_csv(
                f"{filename}/{filename}.csv",
                names=["trade_id", "price", "qty", "quo_qty", "datetime", "isBuyerMaker", "isMatch"]
            )
            df[BUY_SELL] = np.where(df["isBuyerMaker"], "sell", "buy")
            df[DATETIME] = pd.to_datetime(df["datetime"], unit="ms")
            df[SYM] = f"{sym_base}{sym_quote}.BNC"
            df[SYM_BASE] = sym_base
            df[SYM_QUOTE] = sym_quote
            df[EXCH] = BINANCE
            df[AT] = datetime.utcnow()
            df[SYM_ROOT] = sym_root
            df.drop(["isBuyerMaker", "isMatch"], axis=1, inplace=True)
            logger.debug(df.head())
            return df
    
    def fetch_spot_klines(self, 
                          sym_base:str, 
                          sym_quote:str, 
                          freq:str, 
                          dt:Optional[datetime])->pd.DataFrame:
        if not dt:
            dt = datetime.utcnow()
        sym_root = f"{sym_base.upper()}{sym_quote.upper()}"
        dt_str = dt.strftime("%Y-%m-%d")
        endpoint = f"{self.API_URL}/spot/daily/klines/{sym_root}/{freq}"
        filename = f"{sym_root}-{freq}-{dt_str}"
        with self.download_zip(endpoint, filename):
            df = pd.read_csv(
                f"{filename}/{filename}.csv",
                names=["datetime", "open", "high", "low", "close", "volume", "close_dt", "quote_volume", "no_trades", "taker_buy_base", "taker_buy_quote", "/"]
            )
            logger.info(df.head())
            df[DATETIME] = pd.to_datetime(df["datetime"], unit="ms")
            df[SYM] = f"{sym_base}{sym_quote}.{BINANCE}"
            df[SYM_BASE] = sym_base
            df[SYM_QUOTE] = sym_quote
            df[SYM_ROOT] = sym_root
            df[EXCH] = BINANCE
            df[AT] = datetime.utcnow()
            df = df[
                [DATETIME, OPEN, HIGH, LOW, CLOSE, VOL, SYM, SYM_BASE, SYM_QUOTE, SYM_ROOT, \
                 EXCH, AT, "quote_volume", "no_trades", "taker_buy_base", "taker_buy_quote"]
            ]
            logger.info(df.head())
            return df
    

class BNCConnecter(ABCConnection):

    URL = "https://api.binance.com"
    EXCHANGE = "BNC"
    
    def __init__(self) -> None:
        super().__init__("history")
    
    def upsert_symbols(self):
        endpoint = "/api/v3/exchangeInfo"
        ret = self.get(endpoint)
        symbols = ret.json()["symbols"]
        df = pd.DataFrame(symbols)
        df[SYM_BASE] = df["baseAsset"]
        df[SYM_QUOTE] = df["quoteAsset"]
        df[SYM_ROOT] = df["symbol"]
        df[SYM] = df[SYM_ROOT] + f".{BINANCE}"
        df[TICK] = df["quoteAssetPrecision"]
        df[LOT] = df["baseAssetPrecision"]
        df[AT] = datetime.utcnow()
        df[EXCH] = self.EXCHANGE
        df = df[[SYM, SYM_BASE, SYM_QUOTE, SYM_ROOT, TICK, LOT, AT, "status", EXCH]]
        logger.info(df.head())
        self.db_manager.batch_upsert(df.to_dict("records"), "symbols", [SYM])
