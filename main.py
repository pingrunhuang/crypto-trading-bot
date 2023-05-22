from connections.bnc import BNCConn
from eastmoney import calc_funding
from datetime import datetime
import click
import logging

logger = logging.getLogger("main")


@click.command()
@click.option("--sym_base", prompt="symbol base")
@click.option("--sym_quote", default="USDT", prompt="symbol quote")
@click.option("--dt", prompt="date you wanna grab", help="in %Y-%m-%d format")
def store_bnc_daily_trades(sym_base:str, sym_quote:str, dt:str):
    con = BNCConn()
    con.store_spot_trades(sym_base, sym_quote, datetime.strptime(dt, "%Y-%m-%d"))


@click.command()
@click.option("--funcname", prompt="please enter function name you wanna run")
def main(funcname:str):
    if funcname=="store_bnc_daily_trades":
        store_bnc_daily_trades()


if __name__=="__main__":
    main()