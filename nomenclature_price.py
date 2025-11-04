import pandas as pd


def get_nomenclature_price(path):
    data = pd.read_excel(path)
    # print(data)

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
    # print(aggregated_data)

    return aggregated_data
