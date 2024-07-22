import pandas as pd
from src.beehive.beeswax import lineage


def item_to_dataframe(item, conn, interpreter):
    df = conn.execute(interpreter.to_sql(item.source)).df()

    df = df[
        [
            column.name
            for column in item.columns.list_all()
            if type(column) is lineage.dataframes.DataFrameColumn
        ]
    ]

    for column in [
        column
        for column in item.columns.list_all()
        if type(column) is lineage.dataframes.LambdaOutput
    ]:
        print(column.source.function_args)

        df[column.name] = df.apply(
            lambda row: column.source.function(row=row, **column.source.function_args),
            axis=1,
        )

    return df
