import pandas as pd

from .core import Macro
from ...lineage import core as lineage
from ...lineage import values as lineage_values
from ...lineage import functions as lineage_functions
from ...lineage import columns as lineage_columns
from ...lineage import expressions as lineage_expressions
from ...lineage import operators as lineage_operators
from ...lineage import tables as lineage_tables
from ...lineage import joins as lineage_combinations


def json_key(source):
    return lineage_functions.string.Concatenate([lineage_values.Varchar("$."), source])


class JSONKey(Macro):
    def __init__(self, source):
        super().__init__(name="JSONKey", function=json_key, args={"source": source})
