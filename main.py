import asyncio
from connections import historical_downloaders, conns
from datetime import datetime, timedelta
import click
import logging
from config_managers import ConfigManager

logger = logging.getLogger("main")

config_manager = ConfigManager()


async def grab_daily_trades(sym_base:str, 
                            sym_quote:str, 
                            exch:str, 
                            db_name:str, 
                            dt1:datetime,
                            dt2:datetime):
    con = historical_downloaders[exch](db_name)
    while dt1<dt2:
        logger.info(f"Fetching trades of {sym_base}{sym_quote}.{exch} at {dt1}")
        df = await con.fetch_spot_trades(sym_base, sym_quote, dt1)
        await con.upsert_df(df, "trades", ["trade_id"])
        dt1+=timedelta(days=1)

async def grab_klines(sym_base:str,
                 sym_quote:str,
                 freq:str,
                 exch:str,
                 db_name:str,
                 dt1:datetime,
                 dt2:datetime):
    downloader = historical_downloaders[exch](db_name)
    while dt1 <= dt2:
        logger.info(f"Fetching {freq} klines of {sym_base}{sym_quote}.{exch} at {dt1}")
        df = await downloader.fetch_spot_klines(sym_base, sym_quote, freq, dt1)
        await downloader.upsert_df(df, f"bar_{freq}", ["datetime", "symbol"])
        dt1 += timedelta(days=1)


def upsert_symbols(exch:str):
    conn = conns[exch]()
    conn.upsert_symbols()


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
            conn_manager = conns[exch]()
            symbols_map = conn_manager.get_symbol_maps()
            for sym in conf["symbols"]:
                sym_quote = symbols_map[f"{sym}.{exch}"]
                sym_base = sym[:len(sym)-len(f'{sym_quote}')]
                asyncio.run(grab_daily_trades(sym_base, sym_quote, exch, db_name, sdt, edt))
    elif funcname=="grab_klines":
        db_name = "history"
        configs = config_manager.get_kline_config(db_name)
        logger.info(f"configs: {configs}")
        # optimization: use asyncio here
        for conf in configs:
            sdt = conf['sdt']
            edt = conf["edt"] if conf.get("edt") else datetime.utcnow()
            exch = conf["exch"]
            conn_manager = conns[exch]()
            symbols_map = conn_manager.get_symbol_maps()
            for sym in conf["symbols"]:
                sym_quote = symbols_map[f"{sym}.{exch}"]
                sym_base = sym[:len(sym)-len(f'{sym_quote}')]
                for freq in conf["freq"]:
                    asyncio.run(grab_klines(sym_base, sym_quote, freq, exch, db_name, sdt, edt))
    elif funcname == "upsert_symbols":
        for exch in config_manager.get_symbol_config("history"): 
            upsert_symbols(exch)


if __name__=="__main__":
    main()