import logging
from logging import config
import yaml

with open("loggings.yaml") as f:
    configs = yaml.safe_load(f)
    if isinstance(configs, dict):
        config.dictConfig(config=configs)
