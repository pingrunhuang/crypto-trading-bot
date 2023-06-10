import asyncio
import connections
from datetime import datetime, timedelta
import click
import loggers
import logging
from config_managers import ConfigManager
import eastmoney

logger = logging.getLogger("main")

config_manager = ConfigManager()


async def grab_daily_trades(sym_base:str, 
                            sym_quote:str, 
                            exch:str, 
                            db_name:str, 
                            dt1:datetime,
                            dt2:datetime):
    con = connections.historical_downloaders[exch](db_name)
    while dt1<dt2:
        logger.info(f"Fetching trades of {sym_base}{sym_quote}.{exch} at {dt1}")
        df = await con.fetch_spot_trades(sym_base, sym_quote, dt1)
        await con.upsert_df(df, "trades", ["trade_id"])
        dt1+=timedelta(days=1)

def grab_klines(db_name:str):
    async def _grab_klines(sym_base:str,
                    sym_quote:str,
                    freq:str,
                    exch:str,
                    db_name:str,
                    dt1:datetime,
                    dt2:datetime):
        downloader = connections.historical_downloaders[exch](db_name)
        while dt1 <= dt2:
            logger.info(f"Fetching {freq} klines of {sym_base}{sym_quote}.{exch} at {dt1}")
            df = await downloader.fetch_spot_klines(sym_base, sym_quote, freq, dt1)
            await downloader.upsert_df(df, f"bar_{freq}", ["datetime", "symbol"])
            dt1 += timedelta(days=1)

    configs = config_manager.get_kline_config(db_name)
    logger.info(f"configs: {configs}")
    for conf in configs:
        sdt = conf['sdt']
        edt = conf["edt"] if conf.get("edt") else datetime.utcnow()
        exch = conf["exch"]
        conn_manager = connections.conns[exch]()
        symbols_map = conn_manager.get_symbol_maps()
        for sym in conf["symbols"]:
            sym_quote = symbols_map[f"{sym}.{exch}"]
            sym_base = sym[:len(sym)-len(f'{sym_quote}')]
            for freq in conf["freq"]:
                asyncio.run(
                    _grab_klines(sym_base, sym_quote, freq, exch, db_name, sdt, edt)
                )


async def eastmoney_kline(db_name:str):
    configs = config_manager.get_kline_config(db_name)
    logger.info(configs)
    for config in configs:
        logger.debug(f"Start with config: {config}")
        sdt = config['sdt'].strftime("%Y%m%d") if config.get("sdt") else "0"
        edt = config["edt"].strftime("%Y%m%d") if config.get("edt") else "20500101"
        freq = config["freq"]
        for symbol in config["symbols"]:
            await eastmoney.fetch_kline(symbol, freq, sdt, edt, db_name)


def upsert_symbols(exch:str):
    conn = connections.conns[exch]()
    conn.upsert_symbols()


async def run_websockets():
    socket = connections.BNCWebSockets()
    params = {
        "method": "SUBSCRIBE", 
        "params": ["btcusdt@kline_1h"]
    }
    await socket.run("/ws/btcusdt@kline_1h", **params)



@click.command()
@click.option("--funcname", prompt="please enter function name you wanna run")
def main(funcname:str):
    if funcname=="grab_trades":
        db_name = "history"
        configs = config_manager.get_kline_config(db_name)
        for conf in configs:
            sdt = conf['sdt']
            edt = conf["edt"] if conf.get("edt") else datetime.utcnow()
            exch = conf["exch"]
            conn_manager = connections.conns[exch]()
            symbols_map = conn_manager.get_symbol_maps()
            for sym in conf["symbols"]:
                sym_quote = symbols_map[f"{sym}.{exch}"]
                sym_base = sym[:len(sym)-len(f'{sym_quote}')]
                asyncio.run(grab_daily_trades(sym_base, sym_quote, exch, db_name, sdt, edt))
    elif funcname=="grab_klines":
        grab_klines("history")
    elif funcname == "upsert_symbols":
        for exch in config_manager.get_symbol_config("history"): 
            upsert_symbols(exch)
    elif funcname == "websockets":
        asyncio.run(run_websockets())
    elif funcname == "eastmoney_klines":
        db_name = "stock"
        asyncio.run(eastmoney_kline(db_name))

if __name__=="__main__":
    main()