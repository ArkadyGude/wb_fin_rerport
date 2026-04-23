import pandas as pd


def get_nomenclature_price(path):
    data = pd.read_excel(path)

    aggregated_data = (
        data.groupby(
            [
                "Артикул WB",
            ],
            dropna=False,
        )
        .agg(
            {
                "Себестоимость, руб": "mean",
            }
        )
        .reset_index()
    )

    return aggregated_data
