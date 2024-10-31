import yaml
import os
from loggers import LOGGER
from collections import defaultdict

logger = LOGGER

class ConfigManager:

    CONFIG_DIR = "./configs"

    def __init__(self) -> None:
        dirs = os.listdir(self.CONFIG_DIR)
        config = defaultdict(lambda: defaultdict(list))
        for dir in dirs:
            p = os.path.join(self.CONFIG_DIR, dir)
            if not os.path.isdir(p):
                base_filename = os.path.splitext(dir)[0]
                _type, db_name = base_filename.split("_")
                logger.info(f"Loading config from {p}: {_type} {db_name}")
                with open(p, "r") as f:
                    config[_type][db_name] += yaml.safe_load(f)
        self.config = config

    def get_kline_config(self, db_name: str):
        if self.config.get("kline"):
            return self.config["kline"][db_name]
        else:
            raise KeyError(f"Please provide kline config first: {self.config}")

    def get_trade_config(self, db_name: str):
        if self.config.get("trades"):
            return self.config["trades"][db_name]
        else:
            raise KeyError(f"Please provide trades grabber config first: {self.config}")

    def get_symbol_config(self, db_name: str):
        if self.config.get("symbols"):
            return self.config["symbols"][db_name]
        else:
            raise KeyError(
                f"Please provide symbols grabber config first: {self.config}"
            )
