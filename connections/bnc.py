from typing import Optional, Callable
import pandas as pd
import numpy as np
from datetime import datetime, timezone
from io import BytesIO
import shutil
import os
from contextlib import asynccontextmanager
from typing import AsyncIterator
from consts import (
    DATETIME,
    VOL,
    SYM,
    SYM_BASE,
    SYM_QUOTE,
    SYM_ROOT,
    EXCH,
    BINANCE,
    BUY_SELL,
    AT,
    OPEN,
    HIGH,
    LOW,
    CLOSE,
    TICK,
    LOT,
    PROXIES,
    DB_NAME,
)
from connections.base import AsyncBaseConnection, BaseConnection
from utils import unzip
from aiohttp import ClientSession
from loggers import LOGGER

logger = LOGGER

class AsyncBNCConnecter(AsyncBaseConnection):
    """
    binance historical data downloader
    """

    URL = "https://api.binance.com"

    def __init__(self, session: ClientSession, db_name: str = DB_NAME) -> None:
        super().__init__(session, db_name)

    async def fetch_klines(
        self, sym_base: str, sym_quote: str, freq: str, sdt: datetime, edt:Optional[datetime]
    ) -> pd.DataFrame:
        if not edt:
            edt = datetime.now(timezone.utc)

        sym_root = f"{sym_base.upper()}{sym_quote.upper()}"
        endpoint = f"/api/v3/klines"
        logger.info(f"fetching klines from {sdt} to {edt} for {sym_root}")
        sdt_mill = int(sdt.timestamp()*1000)
        edt_mill = int(edt.timestamp()*1000)
        ret = await self.get(endpoint, symbol=sym_root, interval=freq, startTime=sdt_mill, endTime=edt_mill)
        df = pd.DataFrame(ret, columns=["datetime", OPEN, HIGH, LOW, CLOSE, VOL, "datetime2", "quote_volumn", "trades", "taker buy base", "taker buy quote", "-"])
        logger.info(df.head())
        df[DATETIME] = pd.to_datetime(df["datetime"], unit="ms")
        df[SYM] = f"{sym_base}{sym_quote}.{BINANCE}"
        df[SYM_BASE] = sym_base
        df[SYM_QUOTE] = sym_quote
        df[SYM_ROOT] = sym_root
        df[EXCH] = BINANCE
        df[AT] = datetime.now(timezone.utc)
        df = df[
            [
                DATETIME,
                OPEN,
                HIGH,
                LOW,
                CLOSE,
                VOL,
                SYM,
                SYM_BASE,
                SYM_QUOTE,
                SYM_ROOT,
                EXCH,
                AT,
            ]
        ]
        logger.info(df.head())
        return df


class BNCConnecter(BaseConnection):

    URL = "https://api.binance.com"
    EXCHANGE = "BNC"

    def __init__(self) -> None:
        super().__init__("history")

    def upsert_symbols(self):
        endpoint = "/api/v3/exchangeInfo"
        ret = self.get(endpoint, proxies=PROXIES)
        symbols = ret.json()["symbols"]
        df = pd.DataFrame(symbols)
        df[SYM_BASE] = df["baseAsset"]
        df[SYM_QUOTE] = df["quoteAsset"]
        df[SYM_ROOT] = df["symbol"]
        df[SYM] = df[SYM_ROOT] + f".{BINANCE}"
        df[TICK] = df["quoteAssetPrecision"]
        df[LOT] = df["baseAssetPrecision"]
        df[AT] = datetime.now(timezone.utc)
        df[EXCH] = self.EXCHANGE
        df = df[[SYM, SYM_BASE, SYM_QUOTE, SYM_ROOT, TICK, LOT, AT, "status", EXCH]]
        logger.info(df.head())
        self.db_manager.batch_upsert(df.to_dict("records"), "symbols", [SYM])


