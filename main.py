import os
import pandas as pd
import numpy as np

from storage_report import get_storage_report
from advertising_costs import get_data_advert
from fin_report import get_fin_report
from nomenclature_report import get_nomenclature

from utils import (
    get_api_key,
    get_headers,
    count_days_from_last_date,
    get_date_list,
    # get_folder_clients,
    save_data,
)

from nomenclature_price import get_nomenclature_price

from config import (
    # PATH_CLIENTS,
    PATH_PROCESSED,
    FIN_FILE,
    PATH_NOMENCLATURE,
)

from settings import clients_list


def main():
    # folders = get_folder_clients(PATH_CLIENTS)
    folders = clients_list
    print(folders)
    for folder in folders:
        print(folder)
        print(f"Сейчас обрабатывается {folder}. Подождите еще немного...")
        nomenclature_path = os.path.join(folder, PATH_NOMENCLATURE)
        # print(nomenclature_price)
        try:
            data_file_path = os.path.join(folder, PATH_PROCESSED)
            api_key = get_api_key(folder)
            headers = get_headers(api_key)
            if os.path.exists(f"{data_file_path}\\{FIN_FILE}"):
                qty_days = count_days_from_last_date(f"{data_file_path}\\{FIN_FILE}")
                if qty_days == 0:
                    print("Сегодня данные уже загружены.")
                    continue
                # sleep_second = 5
            else:
                qty_days = 95
                # sleep_second = 10
            date_list = get_date_list(qty_days)
            # print(date_list)
            nomenclature = get_nomenclature(headers)
            for one_date in date_list:
                print(one_date)
                fin_report = get_fin_report(folder, headers, one_date, nomenclature)
                storage_report = get_storage_report(folder, headers, one_date)
                if storage_report.empty:
                    report_fin_storage = fin_report.copy()
                    report_fin_storage["Хранение, руб"] = 0.0
                else:
                    report_fin_storage = pd.merge(
                        fin_report, storage_report, on="Артикул WB", how="outer"
                    )

                    report_fin_storage["Хранение, руб"] = (
                        report_fin_storage["Хранение, руб"]
                        .replace("", np.nan)
                        .fillna(0)
                        .infer_objects(copy=False)
                    )

                    report_fin_storage["Дата"] = report_fin_storage["Дата"].fillna(
                        pd.to_datetime(one_date).date()
                    )

                report_advert = get_data_advert(headers, one_date)

                if report_advert == 0:
                    report_fin_storage_advert = report_fin_storage.copy()
                    report_fin_storage_advert["Затраты на рекламу, руб"] = 0.0
                else:
                    # Устанавливаем величину рекламных затрат
                    advertising_cost = float(report_advert)

                    # Создаем объединённую таблицу
                    report_fin_storage_advert = report_fin_storage.copy()

                    # Суммируем продажи и рассчитываем коэффициент распределения расходов
                    total_sales = report_fin_storage_advert["Продажи, руб"].sum()
                    if total_sales > 0:
                        ratio = advertising_cost / total_sales
                    else:
                        ratio = 0

                    # Присваиваем расходы на рекламу согласно продажам
                    report_fin_storage_advert["Затраты на рекламу, руб"] = (
                        report_fin_storage_advert["Продажи, руб"] * ratio
                    )

                print(report_fin_storage_advert["Затраты на рекламу, руб"].sum())

                report_fin_storage_advert["Перечисление на РС, руб"] = (
                    (
                        report_fin_storage_advert["К перечислению за товар, руб"]
                        - report_fin_storage_advert["Логистика, руб"]
                        - report_fin_storage_advert["Штрафы, руб"]
                        - report_fin_storage_advert["Хранение, руб"]
                        - report_fin_storage_advert["Затраты на рекламу, руб"]
                        - report_fin_storage_advert["Платная приемка, руб"]
                    )
                    .replace("", np.nan)
                    .fillna(0)
                    .infer_objects(copy=False)
                )

                report_fin_storage_advert["Налоги, руб"] = (
                    (report_fin_storage_advert["Сумма реализации, руб"] * 0.02)
                    .replace("", np.nan)
                    .fillna(0)
                    .infer_objects(copy=False)
                )

                report_fin_storage_advert["НДС, руб"] = (
                    (report_fin_storage_advert["Сумма реализации, руб"] * 0.05)
                    .replace("", np.nan)
                    .fillna(0)
                    .infer_objects(copy=False)
                )

                nomenclature_price = get_nomenclature_price(nomenclature_path)
                if nomenclature_price.empty:
                    report_fin_full = report_fin_storage_advert.copy()
                    report_fin_full["Себестоимость, руб"] = 0.0
                else:
                    report_fin_full = (
                        pd.merge(
                            report_fin_storage_advert,
                            nomenclature_price,
                            on="Артикул WB",
                            how="outer",
                        )
                        .replace("", np.nan)
                        .fillna(0)
                        .infer_objects(copy=False)
                    )

                report_fin_full["Себестоимость продаж, руб"] = (
                    report_fin_full["Себестоимость, руб"]
                    * report_fin_full["Продажи, шт"]
                ).fillna(0)

                report_fin_full["Чистая прибыль без НДС, руб"] = (
                    (
                        report_fin_full["Перечисление на РС, руб"]
                        - report_fin_full["Себестоимость продаж, руб"]
                        - report_fin_full["Налоги, руб"]
                    )
                    .replace("", np.nan)
                    .fillna(0)
                    .infer_objects(copy=False)
                )

                report_fin_full["Чистая прибыль с НДС(5%), руб"] = (
                    (
                        report_fin_full["Чистая прибыль без НДС, руб"]
                        - report_fin_full["НДС, руб"]
                    )
                    .replace("", np.nan)
                    .fillna(0)
                    .infer_objects(copy=False)
                )

                report_fin_full["Комиссия WB, руб"] = (
                    report_fin_full["Продажи, руб"]
                    - report_fin_full["К перечислению за товар, руб"]
                )

                report_fin_full["Отказы, шт"] = (
                    report_fin_full["Отказы, шт"].fillna(0).astype("int64")
                )

                report_fin_full = report_fin_full[report_fin_full["Дата"] != 0]

                save_data(report_fin_full, data_file_path, FIN_FILE)
                # sleep(sleep_second)

        except Exception as e:
            print("Ошибка в запросе", folder, e)


if __name__ == "__main__":
    main()
