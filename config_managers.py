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
                logger.info(f"Loading config from {p}: {dir}")
                with open(p, "r") as f:
                    config[dir] = yaml.safe_load(f)
        self.config = config

    def get_strategy_config(self, filename: str):
        if self.config.get(filename):
            return self.config[filename]
        else:
            raise KeyError(f"Please provide strategy config {filename} first: {self.config}")