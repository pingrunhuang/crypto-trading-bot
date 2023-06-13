from abc import abstractmethod
import requests
import logging
from mongo_utils import AsyncMongoManager, MongoManager
import pandas as pd
from consts import EXCH, SYM, SYM_QUOTE
from contextlib import asynccontextmanager
from typing import Optional, AsyncIterator
from websockets.client import WebSocketClientProtocol, connect
from aiohttp import ClientSession
from asyncio import Semaphore

logger = logging.getLogger(__name__)

class ABCDownloader:
    
    def __init__(self, sess:ClientSession, db_name:str="history", semaphore:Optional[Semaphore]=None) -> None:
        self.session = sess
        self.db_manager = AsyncMongoManager(db_name)

    async def upsert_df(self, df:pd.DataFrame, clc_name:str, keys:list[str]):
        records = df.to_dict("records")
        logger.info(f"Inserting {len(records)} into {clc_name}")
        await self.db_manager.batch_upsert(records, clc_name, keys)

class ABCConnection:

    URL = ""
    EXCHANGE = ""
    
    def __init__(self, db_name="history"):
        self.db_manager = MongoManager(db_name)

    @abstractmethod
    def ohlcv(self, end_point: str):
        pass
    
    def get(self, endpoint:str, method:str="GET", **kwargs)->requests.Response:
        url = f"{self.URL}{endpoint}"
        params = kwargs.get("params", [])
        ret = requests.request(method, url, params=params)
        if not ret.ok:
            ret.raise_for_status()
        return ret

    def get_symbol_maps(self):
        """
        a map from symbol to symbol quote
        """
        clc = self.db_manager.db.get_collection("symbols")
        symbols = clc.find({EXCH: self.EXCHANGE}, projection=[SYM, SYM_QUOTE])
        result = {x[SYM]: x[SYM_QUOTE] for x in symbols}
        return result
    
    # def place_order(self,)


class ABCWebsockets:
    
    URL = ""
    EXCHANGE = ""
    def __init__(self, 
                 close_timeout:Optional[int]=None, 
                 ping_interval:Optional[int]=30,
                 ping_timeout:Optional[int]=30) -> None:
        self.close_timeout = close_timeout
        self.ping_interval = ping_interval
        self.ping_timeout = ping_timeout

    @asynccontextmanager
    async def open_socket(self, endpoint:str)-> AsyncIterator[WebSocketClientProtocol]:
        url = self.URL + endpoint
        logger.info(f"websocket connecting to {url}")
        socket = await connect(url, 
                               close_timeout=self.close_timeout,
                               ping_interval=self.ping_interval,
                               ping_timeout=self.ping_timeout)
        yield socket
        await socket.close()
    
    async def on_sending(self, 
                         socket:WebSocketClientProtocol,
                         **kwargs):
        raise NotImplementedError()

    async def on_receiving(self, data:dict)->dict:
        raise NotImplementedError("Please implement your decoding method")
    
