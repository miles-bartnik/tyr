import pandas as pd

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
