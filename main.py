import asyncio
import temp
from datetime import datetime, timedelta
import click
import loggers
import logging
from config_managers import ConfigManager
import eastmoney
from eastmoney.trades_processer import trades2mongo
from eastmoney.funds2mongo import funds2mongo
import aiohttp
from alerts import voice_alert


logger = logging.getLogger("main")


config_manager = ConfigManager()


async def grab_daily_trades(
    sym_base: str, sym_quote: str, exch: str, db_name: str, dt1: datetime, dt2: datetime
):
    MAX_CONNECTIONS = 10
    session = aiohttp.ClientSession(
        connector=aiohttp.TCPConnector(limit=MAX_CONNECTIONS, ttl_dns_cache=300)
    )
    con = temp.historical_downloaders[exch](session, db_name)
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
    session = aiohttp.ClientSession(
        connector=aiohttp.TCPConnector(limit=MAX_CONNECTIONS, ttl_dns_cache=300)
    )
    futures = []
    semaphore = asyncio.Semaphore(MAX_CONNECTIONS)

    async def _grab_klines(
        sym_base: str,
        sym_quote: str,
        freq: str,
        exch: str,
        dt1: datetime,
        dt2: datetime,
    ):

        while dt1 <= dt2:
            async with semaphore:
                logger.info(
                    f"Fetching {freq} klines of {sym_base}{sym_quote}.{exch} at {dt1}"
                )
                df = await downloader.fetch_spot_klines(sym_base, sym_quote, freq, dt1)
                await downloader.upsert_df(df, f"bar_{freq}", ["datetime", "symbol"])
                dt1 += timedelta(days=1)

    configs = config_manager.get_kline_config(db_name)

    logger.info(f"configs: {configs}")
    for conf in configs:
        sdt = conf["sdt"]
        edt = conf["edt"] if conf.get("edt") else datetime.utcnow()
        exch = conf["exch"]
        conn_manager = temp.conns[exch]()
        symbols_map = conn_manager.get_symbol_maps()
        downloader = temp.historical_downloaders[exch](session, db_name)

        for sym in conf["symbols"]:
            sym_quote = symbols_map[f"{sym}.{exch}"]
            sym_base = sym[: len(sym) - len(f"{sym_quote}")]
            for freq in conf["freq"]:
                futures.append(_grab_klines(sym_base, sym_quote, freq, exch, sdt, edt))
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
    conn = temp.conns[exch]()
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
            voice_alert(
                f"current price is greater then previous price by {threshold_pct}%"
            )
            raise KeyboardInterrupt()
        return res

    socket = temp.BNCWebSockets()
    pair = "bnbusdt"
    interval = "1h"
    channel = f"{pair}@kline_{interval}"
    params = {"method": "SUBSCRIBE", "params": [channel], "is_breach": is_breach}

    await socket.run(f"/ws/{channel}", **params)


@click.command()
@click.option("--funcname", prompt="please enter function name you wanna run")
def main(funcname: str):
    if funcname == "grab_trades":
        db_name = "hist_data"
        configs = config_manager.get_kline_config(db_name)
        for conf in configs:
            sdt = conf["sdt"]
            edt = conf["edt"] if conf.get("edt") else datetime.utcnow()
            exch = conf["exch"]
            conn_manager = temp.conns[exch]()
            symbols_map = conn_manager.get_symbol_maps()
            for sym in conf["symbols"]:
                sym_quote = symbols_map[f"{sym}.{exch}"]
                sym_base = sym[: len(sym) - len(f"{sym_quote}")]
                asyncio.run(
                    grab_daily_trades(sym_base, sym_quote, exch, db_name, sdt, edt)
                )
    elif funcname == "grab_klines":
        asyncio.run(grab_klines("hist_data"))
    elif funcname == "upsert_symbols":
        for exch in config_manager.get_symbol_config("hist_data"):
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
