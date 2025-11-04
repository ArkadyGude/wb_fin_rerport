import os
import csv
from pathlib import Path
from decouple import AutoConfig
import requests
import pandas as pd

from utils import get_api_key, get_headers
from config import PATH_CLIENTS


url = "https://content-api.wildberries.ru/content/v2/get/cards/list"


def get_folder_clents(path_folder_clients):
    p = Path(path_folder_clients)
    clients = []
    for folder in p.glob("*"):
        client = str(folder)
        clients.append(client)
    return clients


def get_nomenclature(headers):
    folders = get_folder_clents(PATH_CLIENTS)
    for folder in folders:
        try:
            # api_key = get_api_key(folder)
            # headers = get_headers(api_key)

            params = {
                "settings": {"cursor": {"limit": 100}, "filter": {"withPhoto": -1}}
            }

            item_list = []

            res = requests.post(url, headers=headers, json=params, timeout=35)

            print(res.status_code)
            item_list.append(res.json())

            while res.json()["cursor"]["total"] == 100:
                if res.json()["cursor"]["total"] == 100:
                    updatedAt = res.json()["cursor"]["updatedAt"]
                    nmID = res.json()["cursor"]["nmID"]
                    params = {
                        "settings": {
                            "cursor": {
                                "updatedAt": updatedAt,
                                "nmID": nmID,
                                "limit": 100,
                            },
                            "filter": {"withPhoto": -1},
                        }
                    }
                    res = requests.post(url, headers=headers, json=params, timeout=35)

                print(res.status_code)
                item_list.append(res.json())

            nomenclature_list = []

            for k in item_list:
                for item in k["cards"]:
                    nomenclature_dict = {
                        "Артикул WB": item["nmID"],
                        "Предмет": item["subjectName"],
                    }
                    nomenclature_list.append(nomenclature_dict)

            nomenclature_df = pd.DataFrame(nomenclature_list)

            nomenclature_df = nomenclature_df[["Артикул WB", "Предмет"]]
        except Exception as e:
            print("ERROR", e)
    return nomenclature_df

