
import yaml
from bots.okx import OKEXBot
from bots.bnc import BinanceBot
from loggers import LOGGER
from consts import OKEX,BINANCE


BOTS = {
    OKEX: OKEXBot,
    BINANCE: BinanceBot
}