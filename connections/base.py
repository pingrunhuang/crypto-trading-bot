from abc import abstractmethod
import requests
import logging
from mongo_utils import AsyncMongoManager, MongoManger
import pandas as pd
from consts import EXCH, SYM, SYM_QUOTE

logger = logging.getLogger(__name__)

class ABCDownloader:
    
    def __init__(self, db_name:str="history") -> None:
        self.db_manager = AsyncMongoManager(db_name)

    async def upsert_df(self, df:pd.DataFrame, clc_name:str, keys:list[str]):
        await self.db_manager.batch_upsert(df.to_dict("records"), clc_name, keys)

class ABCConnection:

    URL = ""
    EXCHANGE = ""
    
    def __init__(self, db_name="history"):
        self.db_manager = MongoManger(db_name)

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