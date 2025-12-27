from time import sleep
import pandas as pd

from utils import get_data


url_create_report = (
    "https://seller-analytics-api.wildberries.ru/api/v1/acceptance_report"
)

url_check_status = (
    "https://seller-analytics-api.wildberries.ru/api/v1/acceptance_report/tasks/"
)

url_acceptance = (
    "https://seller-analytics-api.wildberries.ru/api/v1/acceptance_report/tasks/"
)


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


def get_acceptance_data(url, task_id, headers):
    """
    Получаем готовый отчет

    """
    url_done_report = url + f"{task_id}/download"

    params = {"task_id": task_id}
    acceptance_done_report = get_data(url_done_report, headers, params)

    print(acceptance_done_report.status_code)

    acceptance_data = pd.json_normalize(acceptance_done_report.json())
    if not acceptance_data.empty:
        acceptance_report = acceptance_data.copy()
        acceptance_report["Артикул WB"] = acceptance_report["nmId"]
        acceptance_report["Приемка, руб"] = acceptance_report["total"]
        acceptance_report = acceptance_report[["Артикул WB", "Приемка, руб"]]

        aggregated_acceptance = (
            acceptance_report.groupby(
                [
                    "Артикул WB",
                ],
                dropna=False,
            )
            .agg(
                {
                    "Приемка, руб": "sum",
                }
            )
            .reset_index()
        )

    else:
        aggregated_acceptance = pd.DataFrame(columns=["Артикул WB", "Приемка, руб"])

    return aggregated_acceptance


def get_acceptance_report(folder, headers, one_date):
    print(
        f"Сейчас обрабатываются затраты на платную приемку {folder}. Подождите еще немного..."
    )
    try:
        params_acceptance_request = {
            "dateFrom": one_date,
            "dateTo": one_date,
        }

        response_create_report = get_data(
            url_create_report, headers, params_acceptance_request
        )

        task_id = response_create_report.json()["data"]["taskId"]
        status_report = get_status_report(url_check_status, task_id, headers)
        done_acceptance_report = get_acceptance_data(url_acceptance, task_id, headers)
    except Exception as e:
        print("ERROR", folder, e)
        done_acceptance_report = pd.DataFrame(columns=["Артикул WB", "Приемка, руб"])

    return done_acceptance_report
