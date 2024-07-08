from .core import Macro
from ...lineage import core as lineage
from ...lineage import values as lineage_values
from ...lineage import functions as lineage_functions
from ...lineage import columns as lineage_columns
from ...lineage import aggregates as lineage_aggregates
from ...lineage import expressions as lineage_expressions
from ...lineage import tables as lineage_tables


def conditional_proportion(condition: lineage.Condition):
    return lineage_functions.Divide(
        left=lineage_functions.Cast(
            source=lineage_functions.Sum(
                lineage.CaseWhen(
                    conditions=[condition],
                    values=[lineage_values.Integer(1)],
                    else_value=lineage_values.Integer(0),
                )
            ),
            data_type=lineage_values.Datatype("FLOAT"),
        ),
        right=lineage_functions.Cast(
            source=lineage_aggregates.Count(source=lineage_values.WildCard()),
            data_type=lineage_values.Datatype("FLOAT"),
        ),
    )


def distinct_proportion(source):
    return lineage_functions.Divide(
        left=lineage_functions.Cast(
            source=lineage_aggregates.Count(distinct=True, source=source),
            data_type=lineage_values.Datatype("FLOAT"),
        ),
        right=lineage_functions.Cast(
            source=lineage_aggregates.Count(source=source),
            data_type=lineage_values.Datatype("FLOAT"),
        ),
    )
