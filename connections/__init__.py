import logging  # this is crucial to have, otherwise this module will just be a placeholder
from consts import BINANCE, OKEX
from connections.bnc import BNCConnecter, AsyncBNCConnecter
from connections.okx import OKEXConnecter

connections = {BINANCE: BNCConnecter, OKEX: OKEXConnecter}
async_connections = {BINANCE: AsyncBNCConnecter}

