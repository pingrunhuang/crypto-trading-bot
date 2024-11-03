
import json
import sys
with open(".vscode/settings.json","r") as f:
    s = json.load(f)
    sys.path.append(*s["python.analysis.extraPaths"])

import click
from loggers import LOGGER
from config_managers import ConfigManager
from bots import BOTS
logger = LOGGER

config_manager = ConfigManager()

@click.command()
@click.option("--config_file", prompt="please enter config file name")
def main(config_file: str):
    logger.info(f"Running with config_file={config_file}...")
    config = config_manager.get_strategy_config(config_file)
    logger.info(f"Found config={config}")
    exch = config["exchange"]
    ws = BOTS[exch](**config)
    ws.run()

if __name__ == "__main__":
    main()
