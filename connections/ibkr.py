"""
This is the connection to interactivebroker api
"""

from base import BaseConnection
from consts import New_York_Stock_Exchange, DB_NAME
import loggers

logger = loggers.logging.getLogger("connections")


class IBKRConnector(BaseConnection):
    URL = "https://localhost:5000/v1/api/"
    EXCHANGE = ""

    def __init__(self, db_name=DB_NAME):
        super().__init__(db_name)
        self.symbols = {}

    def get_symbol_maps(self):
        params = {"exchange": self.EXCHANGE}
        res = self.get("trsrv/all-conids", params=params)
        return res.json()

    def ohlcv(self):
        pass

    def __enter__(self):
        logger.info(f"Start...")

    def __exit__(self, *args, **kwargs):
        logger.info(f"closing session...")
        res = self.get("logout")
        assert res.json().get("confirmed") == True
