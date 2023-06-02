import loggers
from consts import BINANCE
from connections.bnc import BNCConn, BNCDownloader 

conns = {
    BINANCE: BNCConn
}
historical_downloaders = {
    BINANCE: BNCDownloader
}