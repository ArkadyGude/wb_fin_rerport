"""
Получаем параметры РК

1. Получение истории затрат
Метод формирует список фактических затрат на рекламные кампании за заданный период.

Лимит запросов на один аккаунт продавца:
Период 1 секунда
Лимит 1 запрос
Интервал 1 секунда
Всплеск 5 запросов
Минимальный интервал 1 день, максимальный 31

2. Статистика кампаний

Метод формирует статистику для кампаний независимо от типа.
Максимальный период в запросе — 31 день.
Для кампаний в статусах 7, 9 и 11.

Лимит запросов на один аккаунт продавца:
Период 1 минута
Лимит 3 запроса
Интервал 20 секунд
Всплеск 1 запрос

"""

from datetime import timedelta, datetime
from time import sleep
import csv
import os
from pathlib import Path
from decouple import AutoConfig
import requests
import pandas as pd

PATH_CLIENTS = "clients"
PATH_PROCESSED = "processed"

ADVERT_FILE = "advertising.csv"


url_fin = "https://advert-api.wildberries.ru/adv/v1/upd"
url_adv = "https://advert-api.wildberries.ru/adv/v3/fullstats"


def get_data(path, headers, params=None):
    task_completed = False
    res_get = None
    while not task_completed:
        res_get = requests.get(path, headers=headers, params=params, timeout=35)
        try:
            res_get.raise_for_status()
            task_completed = True
        except requests.exceptions.HTTPError as e:
            print(
                f"Код ответа сервера: {e.response.status_code}"
            )  # Мы можем получить код ответа
            if e.response.status_code == 400:
                task_completed = True
            sleep(20)
        except Exception as e:
            print("ERROR", e)
            sleep(20)
    return res_get


def get_adv_list(data):
    adv_list = []
    for k in data.json():
        if k["advertId"] not in adv_list:
            adv_list.append(k["advertId"])
    return adv_list


def process_data_advert(data_advert, date):
    data_list = []
    for item in data_advert.json():
        for item_data in item["days"][0]["apps"]:
            for item_nms in item_data["nms"]:
                data_dict = {
                    "дата": date,
                    "ID рекламной кампании": item["advertId"],
                    "Артикул WB": item_nms["nmId"],
                    "Затраты на рекламу, руб": item_nms["sum"],
                }
                data_list.append(data_dict)
    # print(data_list)
    return data_list


def get_advert_report(folder, headers, one_date):
    print(
        f"Сейчас обрабатываются затраты на рекламу {folder}. Подождите еще немного..."
    )
    try:
        print(one_date)
        day_load = {"from": one_date, "to": one_date}
        get_fin_costs = get_data(url_fin, headers, day_load)
        print(get_fin_costs.status_code)
        adv_list = get_adv_list(get_fin_costs)
        print(adv_list)
        sleep(5)
        list_data_advert = []
        for adv_item in adv_list:
            print(adv_item)
            params_adv = {
                "ids": adv_item,
                "beginDate": one_date,
                "endDate": one_date,
            }

            get_data_advert = get_data(url_adv, headers, params_adv)
            if get_data_advert.status_code == 400:
                continue

            print(get_data_advert.status_code)
            # print(get_data_advert.json())
            data_advert = process_data_advert(get_data_advert, one_date)
            list_data_advert.append(data_advert)
            sleep(20)
        # print(list_data_advert)
        df_advert = pd.DataFrame(
            [item for sublist in list_data_advert for item in sublist]
        )
        if not df_advert.empty:
            advert_report = (
                df_advert.groupby(
                    [
                        "Артикул WB",
                    ],
                    dropna=False,
                )
                .agg(
                    {
                        "Затраты на рекламу, руб": "sum",
                    }
                )
                .reset_index()
            )
            # print(advert_report)
            # print(type(advert_report))
        else:
            advert_report = pd.DataFrame(
                columns=["Артикул WB", "Затраты на рекламу, руб"]
            )

            print(advert_report["Затраты на рекламу, руб"].sum())

    except Exception as e:
        print("ERROR", e)
        advert_report = pd.DataFrame(columns=["Артикул WB", "Затраты на рекламу, руб"])

    return advert_report
