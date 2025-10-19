from .. import values as lineage_values
from .. import functions as lineage_functions


def json_key(source):
    return lineage_functions.string.Concatenate([lineage_values.Varchar("$."), source])
