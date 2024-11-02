from binance import AsyncClient, BinanceSocketManager
import asyncio
import logging

logger = logging.getLogger(__name__)


async def runner():
    client = await AsyncClient.create(api_key="", api_secret="", https_proxy="http://127.0.0.1:7890")
    manager = BinanceSocketManager(client, user_timeout=60)
    kline_manager = manager.trade_socket("BTCUSDT")

    async with kline_manager as m:
        while True:
            res = await m.recv()
            print(res)
    await client.close_connection()


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(runner())
