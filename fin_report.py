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

import pandas as pd

from utils import get_wb_safe


url_fin_report = (
    "https://statistics-api.wildberries.ru/api/v5/supplier/reportDetailByPeriod"
)


def get_insert_fin_data(date, nomenclature):
    df_nomenclature = nomenclature
    df_nomenclature["Дата"] = date
    df_nomenclature["Валюта отчёта"] = "RUB"
    df_nomenclature["Предмет"] = df_nomenclature["Предмет"]
    df_nomenclature["Артикул WB"] = df_nomenclature["Артикул WB"]
    # Целые нули
    df_nomenclature["Отказы, шт"] = 0
    df_nomenclature["Доставки, шт"] = 0
    df_nomenclature["Продажи, шт"] = 0
    df_nomenclature["Возвраты, шт"] = 0
    # Десятичные нули
    df_nomenclature["Продажи, руб"] = 0.0
    df_nomenclature["Возвраты, руб"] = 0.0
    df_nomenclature["Вайлдберриз реализовал Товар (Пр)"] = 0.0
    df_nomenclature["Сумма реализации, руб"] = 0.0
    df_nomenclature["Логистика, руб"] = 0.0
    df_nomenclature["К перечислению за товар, руб"] = 0.0
    df_nomenclature["Штрафы, руб"] = 0.0
    df_nomenclature["Платная приемка, руб"] = 0.0

    df_nomenclature_result = df_nomenclature[
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

    # Приведение типов для гарантии
    df_nomenclature_result["Отказы, шт"] = df_nomenclature_result["Отказы, шт"].astype("Int64")
    df_nomenclature_result["Продажи, шт"] = df_nomenclature_result["Продажи, шт"].astype("Int64")
    df_nomenclature_result["Возвраты, шт"] = df_nomenclature_result["Возвраты, шт"].astype("Int64")
    df_nomenclature_result["Платная приемка, руб"] = df_nomenclature_result["Платная приемка, руб"].astype(float)

    return df_nomenclature_result


def process_fin_report(data):
    """
    Обрабатывает JSON-ответ от API Wildberries /reportDetailByPeriod.
    Возвращает плоский DataFrame с нужными показателями.
    """
    summary_df = pd.json_normalize(data)
    fin_data = summary_df.copy()

    # Даты и строковые поля
    fin_data["Дата"] = summary_df["date_from"]
    fin_data["Валюта отчёта"] = summary_df["currency_name"]
    fin_data["Предмет"] = summary_df["subject_name"]

    # Числовые артикулы
    fin_data["Артикул WB"] = pd.to_numeric(summary_df["nm_id"], errors="coerce").astype("Int64")
    fin_data["Баркод"] = pd.to_numeric(summary_df["barcode"], errors="coerce").astype("Int64")
    fin_data["Бренд"] = summary_df["brand_name"].str.upper()
    fin_data["Размер"] = summary_df["ts_name"]

    # Маски
    mask_sale = summary_df["supplier_oper_name"] == "Продажа"
    mask_return = summary_df["supplier_oper_name"] == "Возврат"
    mask_comp = summary_df["supplier_oper_name"] == "Добровольная компенсация при возврате"
    mask_logistic = summary_df["supplier_oper_name"] == "Логистика"

    # Целые колонки из API
    fin_data["Доставки, шт"] = summary_df["delivery_amount"].astype("Int64")
    fin_data["Отказы, шт"] = summary_df["return_amount"].astype("Int64")

    # Инициализация целых колонок нулями
    fin_data["Продажи, шт"] = 0
    fin_data["Возвраты, шт"] = 0
    # Десятичные
    fin_data["Продажи, руб"] = 0.0
    fin_data["Возвраты, руб"] = 0.0
    fin_data["Вайлдберриз реализовал Товар (Пр)"] = 0.0
    fin_data["Логистика, руб"] = 0.0

    # Заполнение по маскам
    fin_data.loc[mask_sale, "Продажи, шт"] = summary_df.loc[mask_sale, "quantity"]
    fin_data.loc[mask_return, "Продажи, шт"] = -summary_df.loc[mask_return, "quantity"]
    fin_data.loc[mask_return, "Возвраты, шт"] = summary_df.loc[mask_return, "quantity"]

    fin_data.loc[mask_sale, "Продажи, руб"] = (
        summary_df.loc[mask_sale, "retail_price_withdisc_rub"]
        * summary_df.loc[mask_sale, "quantity"]
    )
    fin_data.loc[mask_return, "Продажи, руб"] = (
        -summary_df.loc[mask_return, "retail_price_withdisc_rub"]
        * summary_df.loc[mask_return, "quantity"]
    )
    fin_data.loc[mask_return, "Возвраты, руб"] = -summary_df.loc[mask_return, "retail_amount"]
    fin_data.loc[mask_sale, "Вайлдберриз реализовал Товар (Пр)"] = summary_df.loc[mask_sale, "retail_amount"]
    fin_data.loc[mask_logistic, "Логистика, руб"] = summary_df.loc[mask_logistic, "delivery_rub"]

    # Итоговые расчёты
    fin_data["Сумма реализации, руб"] = (
        fin_data["Вайлдберриз реализовал Товар (Пр)"] + fin_data["Возвраты, руб"]
    )
    fin_data["К перечислению за товар, руб"] = summary_df["ppvz_for_pay"] * (
        mask_sale.astype(int) + mask_comp.astype(int) - mask_return.astype(int)
    )
    fin_data["Штрафы, руб"] = summary_df["penalty"]
    fin_data["Платная приемка, руб"] = summary_df["acceptance"]

    # Приведение типов к требуемым
    # Целые
    fin_data["Продажи, шт"] = fin_data["Продажи, шт"].astype("Int64")
    fin_data["Возвраты, шт"] = fin_data["Возвраты, шт"].astype("Int64")
    fin_data["Отказы, шт"] = fin_data["Отказы, шт"].astype("Int64")
    # Десятичные
    fin_data["Платная приемка, руб"] = fin_data["Платная приемка, руб"].astype(float)

    # Выбор и порядок результирующих колонок
    result = fin_data[
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

    return result


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

    # Повторное приведение типов после агрегации, т.к. sum может изменить тип
    aggregated_data["Продажи, шт"] = aggregated_data["Продажи, шт"].astype("Int64")
    aggregated_data["Возвраты, шт"] = aggregated_data["Возвраты, шт"].astype("Int64")
    aggregated_data["Отказы, шт"] = aggregated_data["Отказы, шт"].astype("Int64")
    aggregated_data["Платная приемка, руб"] = aggregated_data["Платная приемка, руб"].astype(float)

    return aggregated_data


def get_fin_report(folder, headers, one_date, nomenclature):
    print(f"Сейчас обрабатывается финотчет {folder}. Подождите еще немного...")
    fin_report_done = None
    try:
        params_fin_report = {
            "dateFrom": one_date,
            "dateTo": one_date,
            "period": "daily",
        }
        insert_data = get_insert_fin_data(one_date, nomenclature)
        response_fin_report = get_wb_safe(url_fin_report, headers, params_fin_report)
        print(f"response_fin_report {response_fin_report.status_code}")

        if response_fin_report.status_code == 200:
            fin_report = process_fin_report(response_fin_report.json())
            fin_report_done = get_concat_data(insert_data, fin_report)
        elif response_fin_report.status_code == 204:
            fin_report_done = insert_data
        else:
            raise ValueError(
                f"Непредвиденный статус-код API: {response_fin_report.status_code}"
            )
    except Exception as e:
        print("Ошибка в запросе", folder, e)
    return fin_report_done
