import yaml
import pymongo


class MongoManger:
    setting_path = "configs/settings.yaml"
    def __init__(self) -> None:
        with open(self.setting_path) as f:
            config: dict[str, str] = yaml.safe_load(f)
            url = config.get("mgo_url")
        self.mgo_client = pymongo.MongoClient(url)
    
    def get_database(self, name:str="fund"):
        return self.mgo_client.get_database(name)
    
