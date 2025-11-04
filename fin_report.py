"""
Отчёт о продажах по реализации

Метод возвращает детализации к отчётам реализации.

Данные доступны с 29 января 2024 года.

Вы можете выгрузить данные в Google Таблицы
Лимит запросов на один аккаунт продавца:
Период 1 минута
Лимит 1 запрос
Интервал 1 минута
Всплеск 1 запрос

"""

from time import sleep
import pandas as pd
from utils import get_data

url_fin_report = (
    "https://statistics-api.wildberries.ru/api/v5/supplier/reportDetailByPeriod"
)


def get_insert_fin_data(date, nomenclature):
    # df_nomenclature = get_nomenclature(path)
    # df_nomenclature = get_nomenclature(headers)
    df_nomenclature = nomenclature

    # df_nomenclature["Номер счета"] = df_nomenclature["id продавца"].astype(str) + str(
    #     date
    # ).replace("-", "")
    df_nomenclature["Дата"] = date
    df_nomenclature["Валюта отчёта"] = "RUB"
    df_nomenclature["Предмет"] = df_nomenclature["Предмет"]
    df_nomenclature["Артикул WB"] = df_nomenclature["Артикул WB"]
    # df_nomenclature["Бренд"] = df_nomenclature["Бренд"].str.upper()
    # df_nomenclature["Размер"] = df_nomenclature["Размер"]
    # df_nomenclature["Баркод"] = df_nomenclature["Баркод"]
    # df_nomenclature["Продажи, шт"] = 0
    df_nomenclature["Отказы, шт"] = 0
    df_nomenclature["Доставки, шт"] = 0
    df_nomenclature["Продажи, шт"] = 0
    df_nomenclature["Возвраты, шт"] = 0
    df_nomenclature["Продажи, руб"] = 0
    df_nomenclature["Возвраты, руб"] = 0
    df_nomenclature["Вайлдберриз реализовал Товар (Пр)"] = 0
    df_nomenclature["Сумма реализации, руб"] = 0
    df_nomenclature["Логистика, руб"] = 0
    df_nomenclature["К перечислению за товар, руб"] = 0
    df_nomenclature["Штрафы, руб"] = 0
    df_nomenclature["Платная приемка, руб"] = 0

    # print(df_nomenclature.dtypes)

    df_nomenclature_result = df_nomenclature[
        [
            # "Номер счета",
            "Дата",
            "Валюта отчёта",
            "Предмет",
            "Артикул WB",
            "Отказы, шт",
            "Доставки, шт",
            "Продажи, шт",
            "Возвраты, шт",
            "Продажи, руб",
            "Возвраты, руб",
            "Вайлдберриз реализовал Товар (Пр)",
            "Сумма реализации, руб",
            "К перечислению за товар, руб",
            "Логистика, руб",
            "Штрафы, руб",
            "Платная приемка, руб",
        ]
    ]

    # # print(df_nomenclature_result)
    return df_nomenclature_result


def process_fin_report(data):

    summary_df = pd.json_normalize(data)
    fin_data = summary_df

    # # fin_data["Номер счета"] = summary_df["realizationreport_id"]
    # # fin_data["Дата"] = date
    fin_data["Дата"] = summary_df["date_from"]
    # fin_data["Валюта отчёта"] = "RUB"
    fin_data["Валюта отчёта"] = summary_df["currency_name"]
    # fin_data["Номер строки"] = summary_df["rrd_id"]
    # fin_data["Номер поставки"] = summary_df["gi_id"]
    fin_data["Предмет"] = summary_df["subject_name"]
    fin_data["Артикул WB"] = summary_df["nm_id"]
    fin_data["Артикул WB"] = pd.to_numeric(
        fin_data["Артикул WB"], errors="coerce"
    ).astype("Int64")
    fin_data["Бренд"] = summary_df["brand_name"].str.upper()
    # # fin_data["Артикул продавца"] = summary_df["sa_name"]
    fin_data["Размер"] = summary_df["ts_name"]
    fin_data["Баркод"] = summary_df["barcode"]
    fin_data["Баркод"] = pd.to_numeric(fin_data["Баркод"], errors="coerce").astype(
        "Int64"
    )
    # fin_data["Тип документа"] = summary_df["doc_type_name"]
    fin_data["Доставки, шт"] = summary_df["delivery_amount"].astype("Int64")
    fin_data["Отказы, шт"] = summary_df["return_amount"].astype("int64")
    fin_data["Продажи, шт"] = summary_df.apply(
        lambda row: (
            row["quantity"]
            if row["supplier_oper_name"] == "Продажа"
            else (-row["quantity"] if row["supplier_oper_name"] == "Возврат" else 0)
        ),
        axis=1,
    ).astype("Int64")

    fin_data["Возвраты, шт"] = summary_df.apply(
        lambda row: (row["quantity"] if row["supplier_oper_name"] == "Возврат" else 0),
        axis=1,
    ).astype("Int64")

    # fin_data["Цена продавца"] = summary_df["retail_price_withdisc_rub"]
    # fin_data["Продажи, руб"] = summary_df.apply(
    #     lambda row: (
    #         row["retail_price_withdisc_rub"]
    #         * (
    #             row["quantity"](
    #                 -row["quantity"] if row["supplier_oper_name"] == "Возврат" else 0
    #             )
    #         )
    #         if row["supplier_oper_name"] == "Продажа"
    #         else 0
    #     ),
    #     axis=1,
    # )

    fin_data["Продажи, руб"] = summary_df.apply(
        lambda row: (
            row["retail_price_withdisc_rub"] * row["quantity"]
            if row["supplier_oper_name"] == "Продажа"
            else (
                -row["retail_price_withdisc_rub"] * row["quantity"]
                if row["supplier_oper_name"] == "Возврат"
                else 0
            )
        ),
        axis=1,
    )

    fin_data["Возвраты, руб"] = summary_df.apply(
        lambda row: (
            -row["retail_amount"] if row["supplier_oper_name"] == "Возврат" else 0
        ),
        axis=1,
    )
    fin_data["Вайлдберриз реализовал Товар (Пр)"] = summary_df.apply(
        lambda row: (
            row["retail_amount"] if row["supplier_oper_name"] == "Продажа" else 0
        ),
        axis=1,
    )
    fin_data["Сумма реализации, руб"] = (
        fin_data["Вайлдберриз реализовал Товар (Пр)"] + fin_data["Возвраты, руб"]
    )

    fin_data["К перечислению за товар, руб"] = summary_df.apply(
        lambda row: (
            row["ppvz_for_pay"] if row["supplier_oper_name"] == "Продажа" else 0
        ),
        axis=1,
    ) - summary_df.apply(
        lambda row: (
            row["ppvz_for_pay"] if row["supplier_oper_name"] == "Возврат" else 0
        ),
        axis=1,
    )

    fin_data["Логистика, руб"] = summary_df.apply(
        lambda row: (
            row["delivery_rub"] if row["supplier_oper_name"] == "Логистика" else 0
        ),
        axis=1,
    )

    fin_data["Штрафы, руб"] = summary_df["penalty"]
    fin_data["Платная приемка, руб"] = summary_df["acceptance"]

    result_fin_data = fin_data[
        [
            "Дата",
            "Валюта отчёта",
            "Предмет",
            "Артикул WB",
            "Отказы, шт",
            "Доставки, шт",
            "Продажи, шт",
            "Возвраты, шт",
            "Продажи, руб",
            "Возвраты, руб",
            "Вайлдберриз реализовал Товар (Пр)",
            "Сумма реализации, руб",
            "К перечислению за товар, руб",
            "Логистика, руб",
            "Штрафы, руб",
            "Платная приемка, руб",
        ]
    ]

    return result_fin_data


def get_concat_data(insert_data, fin_data):

    data_fin_concat = pd.concat([fin_data, insert_data], axis=0)

    data_fin_reset_index = data_fin_concat.reset_index(drop=True)
    aggregated_data = (
        data_fin_reset_index.groupby(
            [
                "Дата",
                "Валюта отчёта",
                "Предмет",
                "Артикул WB",
            ],
            dropna=False,
        )
        .agg(
            {
                "Отказы, шт": "sum",
                "Доставки, шт": "sum",
                "Продажи, шт": "sum",
                "Возвраты, шт": "sum",
                "Продажи, руб": "sum",
                "Возвраты, руб": "sum",
                "Вайлдберриз реализовал Товар (Пр)": "sum",
                "Сумма реализации, руб": "sum",
                "К перечислению за товар, руб": "sum",
                "Логистика, руб": "sum",
                "Штрафы, руб": "sum",
                "Платная приемка, руб": "sum",
            }
        )
        .reset_index()
    )

    # # Создание копии датафрейма
    df_fin_result = aggregated_data.copy()
    df_fin_result = df_fin_result[df_fin_result["Артикул WB"] != 0]

    return df_fin_result


def get_fin_report(folder, headers, one_date, nomenclature):

    # print(folder)
    print(f"Сейчас обрабатывается финотчет {folder}. Подождите еще немного...")
    try:
        # data_file_path = os.path.join(folder, PATH_PROCESSED)
        params_fin_report = {
            "dateFrom": one_date,
            "dateTo": one_date,
            "period": "daily",
        }
        insert_data = get_insert_fin_data(one_date, nomenclature)
        response_fin_report = get_data(url_fin_report, headers, params_fin_report)
        print(f"response_fin_report {response_fin_report.status_code}")
        fin_report = process_fin_report(response_fin_report.json())
        fin_report_done = get_concat_data(insert_data, fin_report)
        sleep(60)

    except Exception as e:
        print("Ошибка в запросе", folder, e)
    return fin_report_done
