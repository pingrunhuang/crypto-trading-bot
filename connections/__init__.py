import loggers
from consts import BINANCE
from connections.bnc import BNCConnecter, BNCDownloader 

conns = {
    BINANCE: BNCConnecter
}
historical_downloaders = {
    BINANCE: BNCDownloader
}