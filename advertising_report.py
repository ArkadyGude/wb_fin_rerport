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

from time import sleep
import pandas as pd

from utils import get_advert_safe

PATH_CLIENTS = "clients"
PATH_PROCESSED = "processed"

ADVERT_FILE = "advertising.csv"


url_fin = "https://advert-api.wildberries.ru/adv/v1/upd"
url_adv = "https://advert-api.wildberries.ru/adv/v3/fullstats"


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
    return data_list


def get_advert_report(folder, headers, one_date):
    print(
        f"Сейчас обрабатываются затраты на рекламу {folder}. Подождите еще немного..."
    )
    try:
        print(one_date)
        day_load = {"from": one_date, "to": one_date}

        get_fin_costs = get_advert_safe(url_fin, headers, day_load)
        print(f"Статус истории затрат: {get_fin_costs.status_code}")

        if get_fin_costs.status_code == 400:
            print("Нет данных по затратам на рекламу за дату")
            return 0

        adv_list = get_adv_list(get_fin_costs)
        print(f"Кампании: {adv_list}")

        list_data_advert = []

        for adv_item in adv_list:
            params_adv = {
                "ids": adv_item,
                "beginDate": one_date,
                "endDate": one_date,
            }
            get_data_advert = get_advert_safe(url_adv, headers, params_adv)
            if get_data_advert.status_code == 400:
                continue
            data_advert = process_data_advert(get_data_advert, one_date)
            list_data_advert.append(data_advert)
            sleep(1)

        if list_data_advert:
            df_advert = pd.DataFrame(
                [item for sublist in list_data_advert for item in sublist]
            )
            if not df_advert.empty:
                advert_report = (
                    df_advert.groupby("Артикул WB", dropna=False)
                    .agg({"Затраты на рекламу, руб": "sum"})
                    .reset_index()
                )
                cost_advert_report = advert_report["Затраты на рекламу, руб"].sum()
            else:
                cost_advert_report = 0
        else:
            cost_advert_report = 0

        print(f"Сумма затрат на рекламу: {cost_advert_report}")
        return cost_advert_report

    except Exception as e:
        print("Ошибка в get_advert_report:", e)
        return 0
