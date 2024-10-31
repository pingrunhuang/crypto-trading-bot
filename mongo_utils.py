import yaml
from pymongo import MongoClient, UpdateOne
from motor.motor_asyncio import AsyncIOMotorClient
from typing import Optional
from loggers import LOGGER
from consts import DB_NAME


logger = LOGGER


class BaseMongoManager:

    SETTING_PATH = "./credentials.yaml"
    DEFAULT_DB = DB_NAME

    def __init__(self, db_name: Optional[str]) -> None:
        with open(self.SETTING_PATH, mode="r") as f:
            config = yaml.safe_load(f)
        if isinstance(config, dict):
            self.url = config.get("mgo_url")
        else:
            raise ValueError(
                f"Please make sure format of {self.SETTING_PATH} is correct"
            )
        self.mgo_client = None
        self.db = None
        self.setup_db(db_name)

    def setup_db(self, db_name: Optional[str]):
        raise NotImplementedError("Please implement setup_db method")


class MongoManager(BaseMongoManager):

    SETTING_PATH = "./credentials.yaml"

    def __init__(self, db_name: Optional[str]) -> None:
        super().__init__(db_name)

    def setup_db(self, db_name: Optional[str]):
        if self.db is None:
            self.mgo_client = MongoClient(self.url)
            self.db = self.mgo_client.get_database(
                db_name if db_name else self.DEFAULT_DB
            )
            logger.info(f"db setup to {self.db}")

    def batch_insert(self, data: list, clc: str):
        if self.db is not None:
            self.db.get_collection(clc).insert_many(data)
        else:
            msg = "Please provide database name at initialization"
            logger.error(msg)
            raise ValueError(msg)

    def batch_upsert(self, data: list[dict], clc: str, keys: list[str] = ["_id"]):
        to_update = [
            UpdateOne({k: x[k] for k in keys}, {"$set": x}, upsert=True) for x in data
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


class AsyncMongoManager(BaseMongoManager):

    SETTING_PATH = "./credentials.yaml"

    def __init__(self, db_name: str) -> None:
        super().__init__(db_name)

    def setup_db(self, db_name: Optional[str]):
        logger.info(f"Setting up db={db_name}")
        if self.db == None:
            self.mgo_client = AsyncIOMotorClient()
            self.db = self.mgo_client[db_name if db_name else self.DEFAULT_DB]
            logger.info(f"db setup to {self.db}")

    async def batch_upsert(self, data: list[dict], clc: str, keys: list[str] = ["_id"]):
        coll = self.db[clc]
        for doc in data:
            result = await coll.update_one(
                filter={x: doc[x] for x in keys}, update={"$set": doc}, upsert=True
            )
            logger.debug(f"Upserting {doc}: {result}")

    async def batch_insert(self, data: list[dict], clc: str):
        coll = self.db[clc]
        result = await coll.insert_many(data)
        logger.info(f"Inserted {len(result.inserted_ids)} docs")
