import typing

from ... import core as lineage
from ... import expressions as lineage_expressions
from ... import values as lineage_values
from ... import functions as lineage_functions


def add_multiple_columns(columns: lineage.ColumnList):
    added = lineage_functions.math.Add(
        columns.list_columns_()[0], columns.list_columns_()[1]
    )

    for i in range(2, len(columns.list_columns_())):
        added = lineage_functions.math.Add(columns.list_columns_()[i], added)

    return added


def add_multiple_items(items: typing.List):
    added = lineage_functions.math.Add(items[0], items[1])

    for i in range(2, len(items)):
        added = lineage_functions.math.Add(items[i], added)

    return added


def multiply_multiple_columns(columns: lineage.ColumnList):
    multiplied = lineage_functions.math.Multiply(
        columns.list_columns_()[0], columns.list_columns_()[1]
    )

    for i in range(2, len(columns.list_columns_())):
        multiplied = lineage_functions.math.Multiply(
            columns.list_columns_()[i], multiplied
        )

    return multiplied


def multiply_multiple_items(items: typing.List):
    multiplied = lineage_functions.math.Multiply(items[0], items[1])

    for i in range(2, len(items)):
        multiplied = lineage_functions.math.Multiply(items[i], multiplied)

    return multiplied
