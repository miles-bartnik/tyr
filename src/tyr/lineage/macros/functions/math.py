import typing

from ... import core as lineage
from ... import expressions as lineage_expressions
from ... import values as lineage_values
from ... import functions as lineage_functions


def add_multiple_columns(columns: lineage.ColumnList):
    result = None

    added = lineage_functions.math.Add(
        columns.list_columns()[0], columns.list_columns()[1]
    )

    for i in range(2, len(columns.list_columns())):
        added = lineage_functions.math.Add(columns.list_columns()[i], added)

    return added


def add_multiple_variable(variables: typing.List):
    added = lineage_functions.math.Add(variables[0], variables[1])

    for i in range(2, len(variables)):
        added = lineage_functions.math.Add(variables[i], added)

    return added
