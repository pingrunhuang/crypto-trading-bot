import datetime
import json
from loggers import LOGGER
import pandas as pd
import ssl
from bots.base import BaseWebsocket, BaseBot
from datetime import datetime, timedelta
from consts import OKEX
import time
import queue
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass


logger = LOGGER



@dataclass
class Trade:
    dt:     datetime
    sz:     float
    px:     float
    side:   str

    def __init__(self, **kwargs):
        self.dt = datetime.fromtimestamp(int(kwargs['ts'])/1000)
        self.sz = float(kwargs['sz'])
        self.px = float(kwargs['px'])
        self.side = kwargs['side']

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
            trade:Trade = self.data_feed.data_queue.get(block=False)
            last_trade_dt = trade.dt
            cur_price = trade.px
            offset = timedelta(seconds=getattr(self,"stale_time(seconds)"))
            if last_trade_dt-self.dt>=offset:
                raise ValueError(f"Stale time err: {last_trade_dt}-{self.dt}>{offset}")
            else:
                self.dt = last_trade_dt
                pct_change = (cur_price-self.cur_prx)/self.cur_prx
                
                if pct_change > float(self.pct_threshold):
                    self.cur_prx = cur_price
                    logger.info(f"Selling {self.qty} {self.pair}: {trade}")
                elif pct_change < -float(self.pct_threshold):
                    self.cur_prx = cur_price
                    logger.info(f"Buying {self.qty} {self.pair}: {trade}")
                else:
                    logger.info(f"{pct_change}, {self.pct_threshold}")
        except queue.Empty:
            return
        except Exception as e:
            raise e
        finally:
            time.sleep(1)

    def run(self):
        with ThreadPoolExecutor(max_workers=4) as executor:
            try:
                executor.submit(self.data_feed.start)
                while True:
                    f2 = executor.submit(self.generate_signal)
                    result = f2.result()
                    # the signal that used for placing order
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
        self.data_queue = queue.Queue()

    def on_message(self, ws, msg):
        logger.info(f"{self.URL}: {msg}")
        data = json.loads(msg).get("data")
        if not data:
            logger.warning(data)
        else:
            latest = data[0]
            self.data_queue.put(Trade(**latest))

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

