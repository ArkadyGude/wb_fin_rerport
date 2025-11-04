"""
Метод формирует список фактических затрат на рекламные кампании за заданный период.

Лимит запросов на один аккаунт продавца:
Период 1 секунда
Лимит 1 запрос
Интервал 1 секунда
Всплеск	5 запросов

"""

from utils import get_data
from config import URL_ADVERTISING_COSTS


def get_data_advert(headers, date):
    """
    Получаем данные от WB
    """

    params = {"from": date, "to": date}

    get_fin_advert_costs = get_data(URL_ADVERTISING_COSTS, headers=headers, params=params)
    # print(get_fin_advert_costs.json())

    costs = 0
    for item in get_fin_advert_costs.json():
        if item["paymentType"] == "Баланс" or item["paymentType"] == "Счёт":
            costs += item["updSum"]
    # print(costs)
    return costs
