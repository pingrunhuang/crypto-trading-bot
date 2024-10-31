import logging
import logging.config
import yaml

with open("loggings.yaml") as f:
    configs = yaml.safe_load(f)
    if isinstance(configs, dict):
        logging.config.dictConfig(config=configs)

LOGGER = logging.getLogger("main")