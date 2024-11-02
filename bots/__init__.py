
import yaml
from bots.okx import OKEXBot, OKEXSocket
from loggers import LOGGER
from consts import OKEX

def test1():
    with open("configs/settings.yaml") as f:
        setting = yaml.safe_load(f)
        bot = OKEXBot(setting["key"], setting["secret"], setting["passwd"])
    buy_prices = [53250, 53000, 52500]
    sell_prices = [56500, 57000, 57500]
    qty = 0.004
    sizes = []
    for p in buy_prices:
        size = bot.size_validation(qty, 0.00000001, 0.0001)
        sizes.append(size)
        bot.place_order("BTC-USDT", "buy", _type="limit", price=str(p), size=str(size))
    revenue = 0
    for i in range(len(sell_prices)):
        size = sizes[i]
        p = sell_prices[i]
        bot.place_order("BTC-USDT", "sell", _type="limit", price=str(p), size=str(size))
        revenue += (sell_prices[i] - buy_prices[i]) * size
    LOGGER.info(f"Total revenue will be {revenue} USDT")

BOTS = {
    OKEX: OKEXBot
}