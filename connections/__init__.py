import loggers # this is crucial to have, otherwise this module will just be a placeholder
from consts import BINANCE
from connections.bnc import BNCConnecter, BNCDownloader, BNCWebSockets

conns = {
    BINANCE: BNCConnecter
}
historical_downloaders = {
    BINANCE: BNCDownloader
}

ws = {
    BINANCE: BNCWebSockets
}