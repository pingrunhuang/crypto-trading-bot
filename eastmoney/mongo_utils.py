import yaml
from pymongo import MongoClient, UpdateOne
from typing import Union
import logging


logger = logging.getLogger("dev")


class MongoManger:    
    setting_path = "configs/settings.yaml"

    def __init__(self, db_name: Union[str, None]) -> None:
        with open(self.setting_path) as f:
            config = yaml.safe_load(f)
        if isinstance(config, dict):
            url = config.get("mgo_url")
            self.mgo_client = MongoClient(url)
        else:
            raise ValueError(
                f"Please make sure format of {self.setting_path} is correct")
        self.db = None
        self.setup_db(db_name)
    
    def setup_db(self, db_name: Union[str, None]):
        if self.db:
            logger.info(f"db setup to {self.db}")
        else:
            self.db = self.mgo_client.get_database(db_name)

    def batch_insert(self, data: list, clc: str):
        if self.db:
            self.db.get_collection(clc).insert_many(data)
        else:
            msg = "Please provide database name at initialization"
            logger.error(msg)
            raise ValueError(msg)

    def batch_upsert(self, data: list, clc: str, key: str = "_id"):
        to_update = [
            UpdateOne({key: x[key]}, {"$set": x}, upsert=True) for x in data
        ]
        if self.db is not None:
            result = self.db.get_collection(clc).bulk_write(to_update)
            logger.info(f"Inserted count: {result.inserted_count}")
            logger.info(f"Modified count: {result.modified_count}")
            logger.info(f"Upserted count: {result.upserted_count}")
        else:
            msg = "Please call setup_db first"
            logger.error(msg)
            raise ValueError(msg)
