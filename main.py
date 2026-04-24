import os
from time import sleep
import pandas as pd
import numpy as np

from storage_report import get_storage_report
from advertising_report import get_advert_report
from fin_report import get_fin_report
from nomenclature_report import get_nomenclature

from utils import (
    get_api_key,
    get_headers,
    count_days_from_last_date,
    get_date_list,
    save_data,
)

from nomenclature_price import get_nomenclature_price

from config import (
    PATH_PROCESSED,
    FIN_FILE,
    PATH_NOMENCLATURE,
)

from settings import TAX_RATE, VAT_RATE


def main():
    folders = [os.path.join("clients", name) for name in TAX_RATE]
    print(folders)
    for folder in folders:
        name = os.path.basename(folder)
        print(name)
        print(f"Сейчас обрабатывается {name}. Подождите еще немного...")
        nomenclature_path = os.path.join(folder, PATH_NOMENCLATURE)
        try:
            data_file_path = os.path.join(folder, PATH_PROCESSED)
            api_key = get_api_key(folder)
            headers = get_headers(api_key)
            if os.path.exists(f"{data_file_path}\\{FIN_FILE}"):
                qty_days = count_days_from_last_date(f"{data_file_path}\\{FIN_FILE}")
                if qty_days == 0:
                    print("Сегодня данные уже загружены.")
                    continue
                sleep(5)
            else:
                qty_days = 109
            date_list = get_date_list(qty_days)
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

                cost_report_advert = get_advert_report(folder, headers, one_date)
                report_fin_storage_advert = report_fin_storage.copy()
                if cost_report_advert == 0:
                    report_fin_storage_advert["Затраты на рекламу, руб"] = 0.0
                else:
                    total_sales = report_fin_storage_advert["Продажи, руб"].sum()
                    print(f"total sales {total_sales}")
                    if total_sales > 0:
                        ratio = cost_report_advert / total_sales
                        report_fin_storage_advert["Затраты на рекламу, руб"] = (
                            report_fin_storage_advert["Продажи, руб"] * ratio
                        )
                    else:
                        new_df = pd.DataFrame(
                            [
                                {
                                    "Дата": one_date,
                                    "Валюта отчёта": "RUB",
                                    "Затраты на рекламу, руб": cost_report_advert,
                                }
                            ]
                        ).fillna(0)
                        report_fin_storage_advert = pd.concat(
                            [report_fin_storage_advert, new_df], ignore_index=True
                        ).fillna(0)

                print(f'{report_fin_storage_advert["Затраты на рекламу, руб"]}')
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
                    (
                        report_fin_storage_advert["Сумма реализации, руб"]
                        * TAX_RATE[name]
                    )
                    .replace("", np.nan)
                    .fillna(0)
                    .infer_objects(copy=False)
                )

                report_fin_storage_advert["НДС, руб"] = (
                    (report_fin_storage_advert["Сумма реализации, руб"] * VAT_RATE)
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
                sleep(10)

        except Exception as e:
            print("Ошибка в запросе", folder, e)


if __name__ == "__main__":
    main()
