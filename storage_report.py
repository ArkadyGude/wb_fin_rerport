import csv
import os
from pathlib import Path
from time import sleep
from datetime import datetime, timedelta
from decouple import AutoConfig
import requests
import pandas as pd

from utils import (
    get_api_key,
    get_headers,
    count_days_from_last_date,
    get_date_list,
    get_data,
)


PATH_CLIENTS = "clients"
PATH_PROCESSED = "processed"
PATH_NOMENCLATURE = "nomenclature\\wb_nomenclature.XLSX"
FIN_FILE = "fin_report.csv"
STORAGE_FILE = "storage_report_agg.csv"


url_report_storage = "https://seller-analytics-api.wildberries.ru/api/v1/paid_storage"
url_storage = "https://seller-analytics-api.wildberries.ru/api/v1/paid_storage/tasks/"


def get_status_report(url, task_id, headers):
    """
    Проверяем готовность отчета.
    """
    task_completed = False

    while not task_completed:
        url_status_report = url + f"{task_id}/status"

        params_check_status = {"task_id": task_id}
        response_status_task = get_data(url_status_report, headers, params_check_status)

        try:
            response_status_task.raise_for_status()
            if response_status_task.json()["data"]["status"] == "done":
                print(response_status_task.json()["data"]["status"])
                task_completed = True
            elif response_status_task.json()["data"]["status"] != "done":
                print(response_status_task.json()["data"]["status"])
                sleep(5)
        except Exception as e:
            print("ERROR", e)

    return response_status_task.json()["data"]["status"]


def get_storage_data(url, task_id, headers):
    """
    Получаем готовый отчет

    """
    url_done_report = url + f"{task_id}/download"

    params = {"task_id": task_id}
    storage_done_report = get_data(url_done_report, headers, params)

    print(storage_done_report.status_code)

    storage_data = pd.json_normalize(storage_done_report.json())
    if not storage_data.empty:
        storage_report = storage_data.copy()

        storage_report["Артикул WB"] = storage_report["nmId"]
        storage_report["Хранение, руб"] = storage_report["warehousePrice"]
        storage_report = storage_report[["Артикул WB", "Хранение, руб"]]

        aggregated_storage = (
            storage_report.groupby(
                [
                    "Артикул WB",
                ],
                dropna=False,
            )
            .agg(
                {
                    "Хранение, руб": "sum",
                }
            )
            .reset_index()
        )
    else:
        aggregated_storage = pd.DataFrame(columns=["Артикул WB", "Хранение, руб"])

    return aggregated_storage


def get_storage_report(folder, headers, one_date):
    print(f"Сейчас обрабатывается хранение {folder}. Подождите еще немного...")
    try:
        params_storage_request = {
            "dateFrom": one_date,
            "dateTo": one_date,
        }

        response_report_storage = get_data(
            url_report_storage, headers, params_storage_request
        )
        task_id = response_report_storage.json()["data"]["taskId"]
        # print(params_storage_request)
        # print(task_id)
        status_report = get_status_report(url_storage, task_id, headers)
        done_storage_report = get_storage_data(url_storage, task_id, headers)
        sleep(10)

    except Exception as e:
        print("ERROR", folder, e)

    return done_storage_report


# get_storage_report_final()
