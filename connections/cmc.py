import requests
from base import BaseConnection


class CoinMarketCapConnection(BaseConnection):
    URL = "https://api.coinmarketcap.com/data-api/v3/cryptocurrency/listing"

    