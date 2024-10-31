import asyncio
from datetime import datetime, timedelta, timezone
import click
from connections import async_connections, connections
from loggers import LOGGER
from config_managers import ConfigManager
from connections import eastmoney
from connections.base import AsyncBaseConnection
from connections.eastmoney.trades_processer import trades2mongo
from connections.eastmoney.funds2mongo import funds2mongo
import aiohttp
from consts import INTERVAL_MAPPING, DB_NAME
import socket

logger = LOGGER

config_manager = ConfigManager()

async def grab_daily_trades(
    sym_base: str, sym_quote: str, exch: str, db_name: str, dt1: datetime, dt2: datetime
):
    MAX_CONNECTIONS = 10
    session = aiohttp.ClientSession(
        connector=aiohttp.TCPConnector(limit=MAX_CONNECTIONS, ttl_dns_cache=300)
    )
    con = async_connections[exch](session, db_name)
    futures = []
    semaphore = asyncio.Semaphore(MAX_CONNECTIONS)

    async def _grab_daily_trades(dt):
        async with semaphore:
            df = await con.fetch_spot_trades(sym_base, sym_quote, dt)
            await con.upsert_df(df, "trades", ["trade_id"])

    while dt1 < dt2:
        logger.info(f"Fetching trades of {sym_base}{sym_quote}.{exch} at {dt1}")
        futures.append(_grab_daily_trades(dt1))
        dt1 += timedelta(days=1)

    await asyncio.gather(*futures)


async def grab_klines(db_name: str):
    MAX_CONNECTIONS = 10
    
    async with aiohttp.ClientSession(
        connector=aiohttp.TCPConnector(ssl=False, family=socket.AF_INET)
    ) as session:
        futures = []
        semaphore = asyncio.Semaphore(MAX_CONNECTIONS)

        async def _grab_klines(
                conn: AsyncBaseConnection,
                sym_base: str,
                sym_quote: str,
                freq: str,
                exch: str,
                dt1: datetime,
                dt2: datetime,
        ):
            logger.info(f"Start grabbing {freq} bar data of {sym_base}{sym_quote}.{exch} from {dt1} to {dt2}")
            dt = dt1
            while dt <= dt2:
                async with semaphore:
                    edt = dt + INTERVAL_MAPPING[freq]*500
                    edt = min(dt2, edt)
                    logger.info(
                        f"Fetching {freq} klines of {sym_base}{sym_quote}.{exch} from {dt} to {edt} "
                    )
                    df = await conn.fetch_klines(sym_base, sym_quote, freq, dt, edt)
                    await conn.upsert_df(df, f"bar_{freq}", ["datetime", "symbol"])
                    dt += (INTERVAL_MAPPING[freq]*500)

        configs = config_manager.get_kline_config(db_name)
        logger.info(f"Grabbing klines using configs: {configs}")
        for conf in configs:
            sdt = conf["sdt"].replace(tzinfo=None)
            edt = conf["edt"] if conf.get("edt") else datetime.now(timezone.utc)
            edt = edt.replace(tzinfo=None)
            exch = conf["exch"]
            conn_manager = connections[exch]()
            symbols_map = conn_manager.get_symbol_maps()
            conn = async_connections[exch](session, db_name)

            for sym in conf["symbols"]:
                sym_quote = symbols_map[f"{sym}.{exch}"]
                sym_base = sym[: len(sym) - len(f"{sym_quote}")]
                freq = conf["freq"]
                futures.append(_grab_klines(conn, sym_base, sym_quote, freq, exch, sdt, edt))
        await asyncio.gather(*futures)


async def eastmoney_kline(db_name: str):
    configs = config_manager.get_kline_config(db_name)
    logger.info(configs)
    for config in configs:
        logger.debug(f"Start with config: {config}")
        sdt = config["sdt"].strftime("%Y%m%d") if config.get("sdt") else "0"
        edt = config["edt"].strftime("%Y%m%d") if config.get("edt") else "20500101"
        freq = config["freq"]
        for symbol in config["symbols"]:
            await eastmoney.fetch_kline(symbol, freq, sdt, edt, db_name)


def upsert_symbols(exch: str):
    conn = connections[exch]()
    conn.upsert_symbols()


async def run_websockets():

    def is_breach(prev_price: float, cur_price: float, vol: float):

        threshold_pct = 0.2
        tgt_vol = 150000
        res = (
            abs((cur_price - prev_price) * 100 / prev_price) > threshold_pct
            and vol > tgt_vol
        )
        if res:
            logger.info(f"current price is greater then previous price by {threshold_pct}%")
            raise KeyboardInterrupt()
        return res

    pair = "bnbusdt"
    interval = "1h"
    channel = f"{pair}@kline_{interval}"
    params = {"method": "SUBSCRIBE", "params": [channel], "is_breach": is_breach}

    await socket.run(f"/ws/{channel}", **params)


@click.command()
@click.option("--funcname", prompt="please enter function name you wanna run")
def main(funcname: str):
    if funcname == "grab_trades":
        configs = config_manager.get_kline_config(DB_NAME)
        for conf in configs:
            sdt = conf["sdt"]
            edt = conf["edt"] if conf.get("edt") else datetime.now()
            exch = conf["exch"]
            conn_manager = connections[exch]()
            symbols_map = conn_manager.get_symbol_maps()
            for sym in conf["symbols"]:
                sym_quote = symbols_map[f"{sym}.{exch}"]
                sym_base = sym[: len(sym) - len(f"{sym_quote}")]
                asyncio.run(
                    grab_daily_trades(sym_base, sym_quote, exch, db_name, sdt, edt)
                )
    elif funcname == "grab_klines":
        asyncio.run(grab_klines("history"))
    elif funcname == "upsert_symbols":
        for exch in config_manager.get_symbol_config("history"):
            upsert_symbols(exch)
    elif funcname == "websockets":
        asyncio.run(run_websockets())
    elif funcname == "eastmoney_klines":
        db_name = "stock"
        asyncio.run(eastmoney_kline(db_name))
    elif funcname == "eastmoney_trades":
        trades2mongo("eastmoney/data/trades.json")
    elif funcname == "eastmoney_funds":
        funds2mongo("eastmoney/data/funding.json")
    elif funcname == "eastmoney_funding_calc":
        eastmoney.calc_funding()


if __name__ == "__main__":
    main()
