from enum import Enum
# exchanges
BINANCE = "BNC"
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
EXCH= "exchange"
BUY_SELL = "buy_sell"
TICK = "tick_size"
LOT = "lot_size"

class ORDERTYPE(Enum):
    LIMIT="LIMIT"
    MARKET="MARKET"
    STOP_LOSS="STOP_LOSS"
    