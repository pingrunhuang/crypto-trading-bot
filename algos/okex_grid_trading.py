import yaml
from temp.okx import OKEXBot


def run_step_trading():
    with open("configs/grid_trading_params.yaml") as f:
        settings = yaml.safe_load(f)["okex"]
    # start_prx = params.get("initial_price")
    # step_size = params.get("amt_threshold")
    # proxy_host = params.get("proxy_host")
    # proxy_port = params.get("proxy_port")
    with open("configs/settings.yaml") as f:
        params = yaml.safe_load(f)["accounts"]["test"]

    # start_prx = params.get("initial_price")
    # step_size = params.get("amt_threshold")
    bot = OKEXBot(params, settings)
    bot.start_grid_trading()
    # bot.place_order(
    #     "sell", price="3650", size="0.4408180632"
    # )
    # bot.get_balance()


if __name__ == "__main__":
    run_step_trading()
