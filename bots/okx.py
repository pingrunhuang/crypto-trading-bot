import datetime
import json
from loggers import LOGGER
import pandas as pd
import ssl
from bots.base import BaseWebsocket, BaseBot
from datetime import datetime, timezone, timedelta
from consts import OKEX
from threading import Thread
import time
import queue
import sys
from concurrent.futures import ThreadPoolExecutor


logger = LOGGER

class OKEXBot(BaseBot):
    """
    This is a bot that actually generate the signal based on market data feed
    """
    URL = "wss://ws.okx.com:8443/ws/v5/private"
    EXCHANGE = OKEX

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        self.data_feed = OKEXSocket(**kwargs)
        self.dt = datetime.now()
        self.cur_prx = float(self.initial_price)
        self.error_queue = queue.Queue()
    
    def generate_signal(self, *args, **kwargs) -> bool:
        try:
            last_trade_dt = getattr(self.data_feed, "last_dt")
            if last_trade_dt-self.dt>=timedelta(minutes=1):
                raise ValueError(f"Stale time err: {self.data_feed.last_dt}>{self.dt}")
            else:
                self.dt = last_trade_dt
                cur_price = float(self.data_feed.prx)
                pct_change = (cur_price-self.cur_prx)/self.cur_prx
                
                if pct_change > float(self.pct_threshold):
                    self.cur_prx = cur_price
                    logger.info(f"Selling {self.qty}")
                elif pct_change < -float(self.pct_threshold):
                    self.cur_prx = cur_price
                    logger.info(f"Buying {self.qty}")
                else:
                    logger.info(f"{pct_change}, {self.pct_threshold}")
        except Exception as e:
            raise e
        finally:
            time.sleep(1)

    def run(self):
        with ThreadPoolExecutor(max_workers=4) as executor:
            executor.submit(self.data_feed.start)
            while True:
                f2 = executor.submit(self.generate_signal)
                try:
                    result = f2.result()
                except Exception as e:
                    logger.error(e)

    def save(self):
        df = pd.DataFrame(self.orders)
        if df.empty:
            return
        if self.use_batch:
            if len(self.orders) == self.batch_size:
                df.to_csv("orders.csv", mode="a")
                self.orders.clear()
        else:
            df.to_csv("orders.csv", mode="a", header=False)


class OKEXSocket(BaseWebsocket):

    URL = "wss://ws.okx.com:8443/ws/v5/public"
    EXCHANGE = OKEX

    def __init__(
        self,
        **kwargs
    ) -> None:
        super().__init__(**kwargs)

    def on_message(self, ws, msg):
        logger.info(f"{self.URL}: {msg}")
        data = json.loads(msg).get("data")
        if not data:
            logger.warning(data)
        else:
            latest = data[0]
            self.last_dt = datetime.fromtimestamp(int(latest['ts'])/1000)
            self.prx = latest["px"]
            self.size = latest["sz"]
            self.side = latest["side"]


    def get_request_params(self, t:str):
        match t:
            case "trades_data":
                return json.dumps({
                    "op": "subscribe",
                    "args": [
                        {
                        "channel": "trades",
                        "instId": self.pair.upper().strip()
                        }
                    ]
                })
            case "place_order":
                return json.dumps()

    def start(self):
        self.ws_client.run_forever(sslopt={"cert_reqs": ssl.CERT_NONE})

