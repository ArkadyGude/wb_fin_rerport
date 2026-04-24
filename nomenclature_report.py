from pathlib import Path
from time import sleep
import requests
import pandas as pd

from config import URL_NOMENCLATURE


def get_nomenclature(headers):
    limit = 100
    params = {
        "settings": {
            "sort": {"ascending": True},
            "cursor": {"limit": limit},
            "filter": {"withPhoto": -1},
        }
    }

    all_cards = []

    while True:
        res = requests.post(URL_NOMENCLATURE, headers=headers, json=params, timeout=35)
        res.raise_for_status()
        print(res.status_code)
        data = res.json()
        cards = data.get("cards", [])
        all_cards.extend(cards)

        if len(cards) < limit:
            break

        cursor = data["cursor"]
        params["settings"]["cursor"]["updatedAt"] = cursor["updatedAt"]
        params["settings"]["cursor"]["nmID"] = cursor["nmID"]

        sleep(10)

    nomenclature_list = [
        {"Артикул WB": card["nmID"], "Предмет": card["subjectName"]}
        for card in all_cards
    ]

    return pd.DataFrame(nomenclature_list)[["Артикул WB", "Предмет"]]
