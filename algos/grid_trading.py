import time
import logging


logger = logging.getLogger(__name__)


class GridTradingSTY:
    def __init__(self, start_price: float = None):
        self.lowest_price = None
        self.highest_price = None
        self.sym_base = None
        self.sym_quote = None
        self.start_price = (
            (self.lowest_price + self.highest_price) / 2
            if not start_price
            else start_price
        )
        self.interval = 100  # in quote currency

    def sell(self, price: float, amt: float):
        pass

    def buy(self, price: float, amt: float):
        pass

    def get_latest_price(self):
        pass

    def run(self):
        prev_price = 90
        current_price = 100

        while self.highest_price >= current_price >= self.lowest_price:
            if current_price - prev_price >= self.interval:
                self.sell(current_price, current_price / self.interval)
            elif prev_price - current_price >= self.interval:
                self.buy(current_price, current_price / self.interval)
            time.sleep(1)
        else:
            logger.info(f"Price exceeds {self.highest_price} - {self.lowest_price}")
