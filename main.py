from connections import historical_downloaders, conns
from eastmoney import calc_funding
from datetime import datetime, timedelta
import click
import logging
from config_managers import ConfigManager

logger = logging.getLogger("main")

config_manager = ConfigManager()

@click.command()
@click.option("--sym_base", prompt="symbol base")
@click.option("--sym_quote", default="USDT", prompt="symbol quote")
@click.option("--dt", prompt="date you wanna grab", help="in %Y-%m-%d format")
def store_daily_trades(sym_base:str, sym_quote:str, dt:str, exch:str):
    con = historical_downloaders[exch]()
    con.store_spot_trades(sym_base, sym_quote, datetime.strptime(dt, "%Y-%m-%d"))


def store_klines(sym_base:str, sym_quote:str, freq:str, dt:datetime, exch:str, db_name:str):
    logger.info(f"Fetching {freq} klines of {sym_base}{sym_quote}.{exch} at {dt}")
    conn = historical_downloaders[exch](db_name)
    conn.store_spot_klines(sym_base, sym_quote, freq, dt)
    

def upsert_symbols(exch:str):
    conn = conns[exch]()
    conn.upsert_symbols()


@click.command()
@click.option("--funcname", prompt="please enter function name you wanna run")
def main(funcname:str):
    if funcname=="store_bnc_daily_trades":
        store_daily_trades()
    elif funcname=="store_klines":
        configs = config_manager.get_kline_config()
        db_name = "history"
        configs = configs[db_name]
        logger.info(f"configs: {configs}")
        
        # optimization: use asyncio here
        for base in configs:
            sdt = datetime.strptime(configs[base]["sdt"], "%Y-%m-%d")
            edt = datetime.strptime(configs[base]["edt"], "%Y-%m-%d") \
                if configs[base].get("edt") else datetime.utcnow().date()
            exch = configs[base]["exch"]
            for quote in configs[base]["quote"]:
                for freq in configs[base]["freq"]:
                    while sdt <= edt:
                        store_klines(base, quote, freq, sdt, exch, db_name)
                        sdt += timedelta(days=1)
    # elif funcname == "upsert_symbols":
    #     upsert_symbols()

if __name__=="__main__":
    main()