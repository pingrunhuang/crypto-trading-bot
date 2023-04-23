from time import time
import requests
import datetime
import base64
import hashlib
import hmac
import ccxt
import json
from decimal import Decimal
import zlib
from requests.api import head
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
    ENDPOINT = 'https://www.okex.com'

    def __init__(self, confidentials: dict, settings: dict):
        self.confidentials = confidentials
        self.step_size = float(settings["amt_threshold"]) if settings.get(
            "amt_threshold") else 0
        self.pair = settings["pair"] if settings.get("pair") else ""
        self.base_qty = float(
            settings["base_qty"]) if settings.get("base_qty") else 0
        self.cur_prx = float(settings["initial_price"]) if settings.get(
            "initial_price") else self.get_latest_price(self.pair)
        self.side = None

    def place_order(self,
                    side: str,
                    price: str,
                    oid: str = None,
                    _type: str = "limit",
                    size: str = None):
        path = "/api/v5/trade/order"
        url = self.ENDPOINT + path
        sym_base, sym_quote = self.pair.split('-')
        params = {"ordType": _type, "side": side,
                  "instId": self.pair, "sz": size, "px": price}
        params["tdMode"] = "cash"  # only spot now
        params["posSide"] = "net"
        params["tgtCcy"] = sym_quote
        if oid:
            params["clOrdId"] = oid

        logger.debug(f"Params={params}")
        header = generate_header(
            self.confidentials, path, params, "POST", False)
        logger.info(f"Header: {header}")
        res = requests.post(url, json=params, headers=header)
        msg = res.json()
        if msg["code"] != '0':
            raise Exception(msg["message"])
        else:
            logger.info(msg)
            order_id = oid if oid else msg["data"][0]["ordId"]

            return True

    def get_balance(self):
        endpoint = "/api/v5/account/balance"
        url = self.ENDPOINT + endpoint
        header = generate_header(
            self.confidentials, endpoint, method="GET", is_simulated=False)
        logger.info(f"Header: {header}")
        res = requests.get(url, headers=header)
        msg = res.json()
        if msg["code"] == '33017':
            logger.error(msg["message"])
            return pd.DataFrame()
        else:
            data = pd.DataFrame(data=msg["data"][0]["details"])
            logger.debug(f"Balance: {data}")
            return data

    def get_latest_price(self, symbol: str) -> float:
        url = f'{self.ENDPOINT}/api/v5/market/ticker/?instId={symbol}'
        res = requests.get(url)
        if res.ok:
            data = res.json()["data"]
            logger.debug(f'Latest tick of {symbol}: {data}')
            latest_data = data[0]
            return float(latest_data["last"])
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

    def start_grid_trading(self):
        logger.info(f"Start trading {self.pair} with price {self.cur_prx}")
        while True:
            cur_prx = self.get_latest_price(self.pair)
            prx_diff = cur_prx - self.cur_prx
            logger.info(
                f"Last filled price: {self.cur_prx} | Current price: {cur_prx} | Price diff: {prx_diff}")
            base, quote = self.pair.split("-")
            if prx_diff > self.step_size:
                quote_qty = self.base_qty * prx_diff
                self.cur_prx = cur_prx
                logger.info(
                    f"Sell {self.base_qty} {base} at price {cur_prx}: +{quote_qty} {quote}")
                order = self.place_order(self.pair, "sell", str(cur_prx))

            elif prx_diff < -self.step_size:
                quote_qty = self.base_qty * prx_diff
                self.cur_prx = cur_prx
                logger.info(
                    f"Buy {self.base_qty} {base} at price {cur_prx}: -{quote_qty} {quote}")
                order = self.place_order(self.pair, "buy", str(cur_prx))

            time.sleep(5)


class OKEXSocket(websocket.WebSocketApp):

    def __init__(
            self,
            pair: str = None,
            url: str = None,
            cur_prx: float = None,
            base_qty: float = 0,
            quote_qty: float = 0,
            threshold: float = None,
            proxy_host: str = None,
            proxy_port: str = None,
            step_size: float = None) -> None:

        self.url = url if url else "wss://ws.okex.com:8443/ws/v5/public"
        self.pair = pair if pair else "BTC-USDC"
        self.channels = f"spot/trade:{self.pair}"

        self.cur_prx = cur_prx
        self.base_qty = base_qty
        self.quote_qty = quote_qty
        self.orders = []
        self.batch_size = 10
        self.use_batch = False
        self.threshold = threshold if threshold else 0.1
        self.step_size = step_size if step_size else 1000
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
        self.step_chg_sty(data)
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

    def pct_chg_sty(self, data: dict):
        cur_price = float(data[0]["price"])
        if not self.cur_prx:
            self.cur_prx = cur_price
        logger.info(
            f"Last filled price: {self.cur_prx} | Current price: {cur_price}")
        diff_pct = (cur_price - self.cur_prx)/self.cur_prx
        if diff_pct > self.threshold:
            logger.info(
                f"Sell {self.base_qty} at price {cur_price}: {cur_price-self.cur_prx}")
            self.orders.append({"qty": self.base_qty, "ntl": self.base_qty *
                               (cur_price - self.cur_prx), "price": cur_price})
            self.cur_prx = cur_price
        elif diff_pct < -self.threshold:
            logger.info(
                f"Buy {self.base_qty} at price {cur_price}: {cur_price-self.cur_prx}")
            self.orders.append({"qty": self.base_qty, "ntl": self.base_qty *
                               (cur_price - self.cur_prx), "price": cur_price})
            self.cur_prx = cur_price

    def step_chg_sty(self, data: dict):
        cur_price = float(data[0]["price"])
        if not self.cur_prx:
            self.cur_prx = cur_price
        logger.info(
            f"Last filled price: {self.cur_prx} | Current price: {cur_price}")
        base, quote = self.pair.split("-")
        prx_diff = cur_price - self.cur_prx
        if prx_diff > self.step_size:
            quote_qty = self.base_qty * prx_diff
            logger.info(
                f"Sell {self.base_qty} {base} at price {cur_price}: +{quote_qty} {quote}")
            self.orders.append(
                {"qty": self.base_qty, "quote_qty": quote_qty, "price": cur_price})
            self.cur_prx = cur_price
        elif prx_diff < -self.step_size:
            quote_qty = self.base_qty * prx_diff
            logger.info(
                f"Buy {self.base_qty} {base} at price {cur_price}: -{quote_qty} {quote}")
            self.orders.append(
                {"qty": self.base_qty, "quote_qty": quote_qty, "price": cur_price})
            self.cur_prx = cur_price

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
        bot.order("BTC-USDT", "buy", _type="limit",
                  price=str(p), size=str(size))
    revenue = 0
    for i in range(len(sell_prices)):
        size = sizes[i]
        p = sell_prices[i]
        bot.order("BTC-USDT", "sell", _type="limit",
                  price=str(p), size=str(size))
        revenue += (sell_prices[i] - buy_prices[i]) * size
    logger.info(f"Total revenue will be {revenue} USDT")


def test2():
    with open("configs/grid_trading_params.yaml") as f:
        params = yaml.safe_load(f)["okex"]
    start_prx = params.get("initial_price")
    proxy_host = params.get("proxy_host")
    proxy_port = params.get("proxy_port")
    ws = OKEXSocket(pair=params["pair"],
                    cur_prx=float(start_prx) if start_prx else None,
                    threshold=float(params["pct_threshold"]),
                    proxy_host=proxy_host,
                    proxy_port=proxy_port)
    ws.start()
