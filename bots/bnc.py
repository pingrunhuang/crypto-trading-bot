from bots.base import BaseWebsocket, BaseBot
"""


A single connection is only valid for 24 hours; expect to be disconnected at the 24 hour mark

The websocket server will send a ping frame every 3 minutes. If the websocket server does not receive a pong frame back from the connection within a 10 minute period, the connection will be disconnected. Unsolicited pong frames are allowed(the client can send pong frames at a frequency higher than every 15 minutes to maintain the connection).

WebSocket connections have a limit of 10 incoming messages per second.

"""
from loggers import LOGGER
from concurrent.futures import ThreadPoolExecutor
import ssl
from dataclasses import dataclass
from datetime import datetime, timedelta
import json
import time
import queue


@dataclass
class Trade:
    price: float
    qty: float
    dt: datetime
    side: str
    def __init__(self, **kwargs):
        self.dt = datetime.fromtimestamp(int(kwargs['t'])/1000)
        self.qty = float(kwargs['q'])
        self.price = float(kwargs['p'])
        self.side = "buy" if  kwargs['m'] else "sell"


class BinanceBot(BaseBot):
    URL = "wss://testnet.binance.vision/ws"

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.socket = BinanceSocket(**kwargs)
        self.cur_prx = self.initial_price
        self.dt = datetime.now()
    
    def on_error(self, msg):
        LOGGER.error(msg)
        self.raise_error(msg)
    
    def generate_signal(self):
        while True:
            try:
                trade:Trade = self.socket.data_queue.get(block=True)
                LOGGER.info(trade)
                last_trade_dt = trade.dt
                cur_price = trade.price
                offset = timedelta(seconds=getattr(self,"stale_time(seconds)"))
                if last_trade_dt-self.dt>=offset:
                    raise ValueError(f"Stale time err: {last_trade_dt}-{self.dt}>{offset}")
                else:
                    pct_change = (cur_price-self.cur_prx)/self.cur_prx
                    
                    if pct_change > float(self.pct_threshold):
                        self.dt = last_trade_dt
                        self.cur_prx = cur_price
                        LOGGER.info(f"Selling {self.qty} {self.pair}: {trade}")
                    elif pct_change < -float(self.pct_threshold):
                        self.dt = last_trade_dt
                        self.cur_prx = cur_price
                        LOGGER.info(f"Buying {self.qty} {self.pair}: {trade}")
                    else:
                        LOGGER.info(f"{pct_change}, {self.pct_threshold}")
            except queue.Empty:
                return
            except Exception as e:
                self.raise_error(e)
            finally:
                time.sleep(1)

    def run(self):
        with ThreadPoolExecutor(max_workers=2) as executor:
            executor.submit(self.socket.start)
            executor.submit(self.generate_signal)
            while True:
                while not self.error_queue.empty() or not self.socket.error_queue.empty():
                    err = self.error_queue.get()
                    LOGGER.error(err)
                    err = self.socket.error_queue.get()
                    LOGGER.error(err)
                    time.sleep(0.1)



class BinanceSocket(BaseWebsocket):

    URL = "wss://stream.binance.com:9443/ws/"
    # URL = "wss://stream.testnet.binance.vision/ws"

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        self.ws_client.url = self.URL+f"{self.pair.lower()}@trade"

    def on_message(self, ws, msg):
        LOGGER.info(msg)
        data = json.loads(msg)
        t = Trade(**data)
        self.data_queue.put(t)

    def start(self):
        self.ws_client.run_forever(sslopt={"cert_reqs": ssl.CERT_NONE})
    
    def on_ping(self, ws, msg):
        self.ws_client.send("ping")
    
    def subscribe(self, channel):
        self.ws_client.send()
