import yaml
import logging
import pymongo
import pandas as pd

logging.basicConfig(level=logging.DEBUG, format="%(asctime)s|%(levelname)s|%(message)s")

logger = logging.getLogger(__name__)

class MongoManger:
    setting_path = "configs/settings.yaml"
    def __init__(self) -> None:
        with open(self.setting_path) as f:
            config: dict[str, str] = yaml.safe_load(f)
            url = config.get("mgo_url")
        self.mgo_client = pymongo.MongoClient(url)
    
    def get_database(self):
        return self.mgo_client.get_database("fund")
    

def trades2mongo(historical_json:str):
    import json
    
    with open(historical_json, encoding="utf-8") as fp:
        historical = json.load(fp)
    df = pd.DataFrame(historical)
    column_mapping = {
        "Cjbh": "trade_id",
        "Zqdm": "symbol_id",
        "Zqmc": "symbol",
        "Cjjg": "trade_prx",
        "Cjsl": "trade_amt",
        "Cjje": "trade_cny",
        "Sxf": "trade_fee",
        "Yhs": "stamp_tax",
        "Ghf": "transmit_fee",
        "Jygf": "regulate_fee",
        "Gfye": "position",
        "Zjye": "balance",
        "Mmlb": "buysell"
    }
    df.rename(column_mapping, inplace=True, axis=1)
    df["datetime"] = pd.to_datetime(df["Cjrq"] + df["Cjsj"], format="%Y%m%d%H%M%S")
    columns:list = list(column_mapping.values()) + ["datetime"]
    data = df[columns].drop_duplicates()
    numeric_cols = [
        "trade_prx", "trade_amt", "trade_fee", "stamp_tax", "transmit_fee", "regulate_fee", "position", "balance"
    ]
    data[numeric_cols] = data[numeric_cols].astype(float)
    mgo_manager = MongoManger()
    clc = mgo_manager.get_database().get_collection("em_trades")
    clc.insert_many(data.to_dict("records"))

def calc_total_position(data:list):
    total_buy = 0
    total_sell = 0
    position = {}
    for entry in data:
        if entry["Mmlb"] == "B":
            total_buy+=float(entry["Cjje"])
            ntl = -float(entry["Cjje"])
        elif entry["Mmlb"] == "S":
            total_sell+=float(entry['Cjje'])
            ntl = float(entry["Cjje"])
        else:
            ntl = 0
        position[entry["Zqmc"]] = float(position.get(entry["Zqmc"], 0)) + ntl
    
    logger.info(f"Total buy: {total_buy}")
    logger.info(f"Total sell: {total_sell}")
    logger.info(f"Position: {position}")
    logger.info(f"Total money spent: {total_buy-total_sell}")
    logger.info(f"Last datetime: {data[0]['Cjrq']}")
    return total_buy-total_sell
    

def calc_pnl(data:dict, assets_ntl:float):
    latest_assets_ntl = float(data["Data"][0]["Zzc"])
    logger.info(f"Total pnl: {latest_assets_ntl-assets_ntl}")
    

def calc_funding(data:dict):
    total = 0
    for batch in data:
        for entry in batch["Data"]:
            total += float(entry["Zzje"])
    logger.info(f"Total funding: {total}")


if __name__=="__main__":
    data = "data/data.json"
    trades2mongo(data)