from enum import Enum
from datetime import timedelta

# exchanges
BINANCE = "BNC"
OKEX = "OKX"
NASDAQ = "NASDAQ"
New_York_Stock_Exchange = "NYSE"
HK_Stock_Exchange = "SEHK"


# columes
CLOSE = "close"
OPEN = "open"
HIGH = "high"
LOW = "low"
VOL = "volume"
SYM = "symbol"
SYM_ROOT = "symbol_root"
SYM_BASE = "symbol_base"
SYM_QUOTE = "symbol_quote"
DATETIME = "datetime"
AT = "update_at"
EXCH = "exchange"
BUY_SELL = "buy_sell"
TICK = "tick_size"
LOT = "lot_size"


class ORDERTYPE(Enum):
    LIMIT = "LIMIT"
    MARKET = "MARKET"
    STOP_LOSS = "STOP_LOSS"

PROXIES = {
    "http": "127.0.0.1:7890",
    "https": "127.0.0.1:7890"
}
AIO_PROXIES = "http://127.0.0.1:7890"

INTERVAL_MAPPING = {
    "1m": timedelta(seconds=60),
    "1h": timedelta(hours=1),
    "4h": timedelta(hours=4),
    "1d": timedelta(days=1)
}
DB_NAME = "history"