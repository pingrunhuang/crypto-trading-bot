from sys import flags
from time import time
from typing import Callable
import requests
import datetime
import base64
import hashlib
import hmac
import ccxt
import json
from decimal import Decimal
import zlib
import websocket
from websocket import create_connection, send, recv
import yaml
import _thread as thread
import time
import logging

logger = logging.getLogger(__name__)

logger.add("app.log")

def inflate(data):
    decompress = zlib.decompressobj(
            -zlib.MAX_WBITS
    )
    inflated = decompress.decompress(data)
    inflated += decompress.flush()
    return inflated


class OKEXBot:
    ENDPOINT = 'https://www.okexcn.com'
    # ENDPOINT = 'https://www.okex.com'

    def __init__(self, key: str, secret: str, phrase: str):
        self.key = key
        self.secret = secret
        self.phrase = phrase
        self.cli = ccxt.okex(config={"apiKey": key, "secret": secret, "password": phrase, "hostname": "okexcn.com"})

    def batch_order(self):
        url = f'{self.ENDPOINT}/api/spot/v3/batch_orders'

    def order(self,
              symbol: str,
              side: str,
              oid: str = None,
              _type: str = "limit",
              price: str = None,
              ntl: str = None,
              size: str = None):
        path = "/api/spot/v3/orders"
        url = self.ENDPOINT + path
        params = {"type": _type, "side": side, "instrument_id": symbol}
        if oid:
            params["client_oid"] = oid
        if _type == 'limit' and price is not None and size is not None:
            params["price"] = price
            params["size"] = size
        elif _type == 'market' and side == 'sell' and size:
            params["size"] = size
        elif _type == 'market' and side == 'buy' and ntl:
            params["notional"] = ntl
        else:
            raise ValueError(f"Please check type={_type}, size={size}, price={price}, notional={ntl}")
        logger.debug(f"Params={params}")
        sign = self.cli.sign("orders", method="POST", params=params, api="spot")
        res = requests.post(sign["url"], data=sign["body"], headers=sign["headers"])
        msg = res.json()
        if msg["code"] == '33017':
            logger.error(msg["message"])
            return False
        else:
            logger.info(msg)
            return True

    def get_latest_price(self, symbol) -> float:
        url = f'{self.ENDPOINT}/api/spot/v3/instruments/{symbol}/ticker'
        res = requests.get(url)
        if res.ok:
            data = res.json()
            logger.debug(f'Latest tick of {symbol}: {data}')
            return float(data["last"])
        else:
            logger.error(f"Error: {res.content}")
            return -1

    def size_validation(self, size: float, size_increment: float, min_size: float) -> float:
        if size <= min_size:
            return min_size
        multiplier = size//size_increment
        if multiplier <= 1:
            return size_increment
        # precition = str(size_increment) - 2 if size_increment < 0 else 0
        return multiplier*size_increment


class OKEXSocket(websocket.WebSocketApp):

    def __init__(
        self, 
        pair: str = None, 
        url: str = None, 
        start_price: float = None,
        threshold: float = None,
        proxy_host: str = None,
        proxy_port: str = None) -> None:

        self.url = url if url else "wss://real.okex.com:8443/ws/v3"
        self.channels = f"spot/trade:{pair}" if pair else "spot/trade:BTC-USDC"
        self.start_price = start_price
        self.base_qty = 0.004
        self.orders = []
        self.batch_size = 10
        self.use_batch = False
        self.threshold = threshold if threshold else 0.1
        self.proxy_host = proxy_host
        self.proxy_port = proxy_port
        super().__init__(
            self.url, 
            on_open=self.on_open, 
            on_message=self.on_message,
            on_error=self.on_error,
            on_close=self.on_close,
            on_ping=self.on_ping,
            on_pong=self.on_pong
        )

    def on_message(self, msg):
        msg = inflate(msg)
        logger.info(f"{self.url}: {msg}")
        data = json.loads(msg)["data"]
        self.order(data)
        self.save()

    def save(self):
        df = pd.DataFrame(self.orders)
        if df.empty:
            return 
        if self.use_batch:
            if len(self.orders) == self.batch_size:
                df.to_csv("orders.csv", mode='a')
                self.orders.clear()
        else:
            df.to_csv("orders.csv", mode='a', header=False)

    def on_error(self, err):
        logger.error(f"{self.url}: {err}")

    def on_close(self):
        super().close()
        logger.info("closed socket")

    def on_ping(self, msg):
        logger.info(f"ping {msg}")

    def on_pong(self, msg):
        logger.info(f"pong {msg}")

    def on_open(self):
        websocket.enableTrace(True)
        thread.start_new_thread(self.subscribe, ())
        
    def subscribe(self):
        params = {"op": "subscribe", "args": self.channels}
        str_params = json.dumps(params)
        logger.debug(f"Sending {str_params}")
        self.send(str_params)

    def order(self, data: dict):
        cur_price = float(data[0]["price"])
        if not self.start_price:
            self.start_price = cur_price
        logger.info(f"Start price: {self.start_price} | Current price: {cur_price}")
        diff_pct = (cur_price - self.start_price)/self.start_price
        if diff_pct > self.threshold:
            logger.info(f"Sell {self.base_qty} at price {cur_price}: {cur_price-self.start_price}")
            self.orders.append({"qty": self.base_qty, "ntl": self.base_qty*(cur_price - self.start_price), "price": cur_price})
            self.start_price = cur_price
        elif diff_pct < -self.threshold:
            logger.info(f"Buy {self.base_qty} at price {cur_price}: {cur_price-self.start_price}")
            self.orders.append({"qty": self.base_qty, "ntl": self.base_qty*(cur_price - self.start_price), "price": cur_price})
            self.start_price = cur_price
    
    def run_forever(self):
        if self.proxy_port and self.proxy_host:
            super().run_forever(http_proxy_host=self.proxy_host, http_proxy_port=self.proxy_port)
        else:
            super().run_forever()

    def start(self):
        while True:
            try:
                self.run_forever()
            except KeyboardInterrupt:
                break

def test1():
    import yaml
    with open("configs/settings.yaml") as f:
        setting = yaml.load(f)
    bot = OKEXBot(setting["key"], setting["secret"], setting["passwd"])
    buy_prices = [53250, 53000, 52500]
    sell_prices = [56500, 57000, 57500]
    qty = 0.004
    sizes = []
    for p in buy_prices:
        size = bot.size_validation(qty, 0.00000001, 0.0001)
        sizes.append(size)
        bot.order("BTC-USDT", "buy", _type="limit", price=str(p), size=str(size))
    revenue = 0
    for i in range(len(sell_prices)):
        size = sizes[i]
        p = sell_prices[i]
        bot.order("BTC-USDT", "sell", _type="limit", price=str(p), size=str(size))
        revenue += (sell_prices[i] - buy_prices[i]) * size
    logger.info(f"Total revenue will be {revenue} USDT")


def test2():
    with open("configs/grid_trading_params.yaml") as f:
        params = yaml.safe_load(f)["okex"]
    start_prx = params.get("initial_price")
    ws = OKEXSocket(pair=params["pair"], 
                    start_price=float(start_prx) if start_prx else None, 
                    threshold=float(params["pct_threshold"]),
                    proxy_host=params["proxy_host"],
                    proxy_port=params["proxy_port"])
    ws.start()


if __name__ == '__main__':
    test2()