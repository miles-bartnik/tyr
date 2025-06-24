import pandas as pd
import re
import os
from importlib import resources


def timestamp(timestamp_format, output_syntax):
    with resources.path("syntax.translations", "timestamp.tsv") as df:
        mappings = pd.read_csv(df, delimiter="\t")

    for index, row in mappings.iterrows():
        timestamp_format = re.sub(timestamp_format, row["duckdb"], row[output_syntax])

    return timestamp_format
