from abc import abstractmethod
import requests
import logging
from mongo_utils import MongoManger

logger = logging.getLogger(__name__)

class ABCConnection:
    URL = ""
    def __init__(self, clc_name="history"):
        self.db_manager = MongoManger("history")

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