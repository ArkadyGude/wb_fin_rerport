import os
from pathlib import Path
from time import sleep
from datetime import datetime, timedelta
from decouple import AutoConfig
import requests
import pandas as pd


def get_folder_clients(path_folder_clients):
    p = Path(path_folder_clients)
    clients = []
    for folder in p.glob("*"):
        client = str(folder)
        clients.append(client)
    return clients


def get_api_key(path):
    config = AutoConfig(search_path=path)
    api_key = config("api_key")
    return api_key


def get_headers(api_key):
    headers = {"Authorization": api_key}
    return headers


def count_days_from_last_date(csv_file):
    df = pd.read_csv(csv_file, sep=";")
    df["Дата"] = pd.to_datetime(df["Дата"], format="%Y-%m-%d")
    last_date_str = df["Дата"].iloc[-1]
    today = datetime.now().date()
    last_date = last_date_str.date()
    delta = today - last_date
    return delta.days - 1


def get_date_list(num_days=0):
    date_from = datetime.now() - timedelta(days=num_days)
    date_to = datetime.now()
    step = timedelta(days=1)
    date_list = []
    while date_from < date_to:
        date_list.append(date_from.strftime("%Y-%m-%d"))
        date_from += step
    return date_list



def get_data(path, headers, params=None):
    task_completed = False
    res_get = None
    while not task_completed:
        res_get = requests.get(path, headers=headers, params=params, timeout=35)
        try:
            res_get.raise_for_status()
            task_completed = True
        except Exception as e:
            print("ERROR", e)
            sleep(20)
    return res_get


def save_data(data, path, name_file):
    path_file = os.path.join(path, name_file)
    if not (os.path.exists(path_file)):
        data.to_csv(
            path_file,
            mode="w",
            header=data.columns,
            index=False,
            date_format="%Y-%m-%d",
            sep=";",
            encoding="utf-8-sig",
        )
    else:
        data.to_csv(
            path_file,
            mode="a",
            header=False,
            index=False,
            date_format="%Y-%m-%d",
            sep=";",
            encoding="utf-8-sig",
        )
