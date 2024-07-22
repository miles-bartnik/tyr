from src.beehive.beeswax.lineage.macros.core import Macro
from src.beehive.beeswax.lineage import core as lineage
from src.beehive.beeswax.lineage import values as lineage_values
from src.beehive.beeswax.lineage import functions as lineage_functions
from src.beehive.beeswax.lineage import columns as lineage_columns
from src.beehive.beeswax.lineage import expressions as lineage_expressions
from src.beehive.beeswax.lineage import tables as lineage_tables


def conditional_proportion(condition: lineage.Condition):
    return lineage_functions.math.Divide(
        left=lineage_functions.data_type.Cast(
            source=lineage_functions.aggregate.Sum(
                lineage.CaseWhen(
                    conditions=[condition],
                    values=[lineage_values.Integer(1)],
                    else_value=lineage_values.Integer(0),
                )
            ),
            data_type=lineage_values.Datatype("FLOAT"),
        ),
        right=lineage_functions.data_type.Cast(
            source=lineage_functions.aggregate.Count(source=lineage_values.WildCard()),
            data_type=lineage_values.Datatype("FLOAT"),
        ),
    )


def distinct_proportion(source):
    return lineage_functions.math.Divide(
        left=lineage_functions.data_type.Cast(
            source=lineage_functions.aggregate.Count(distinct=True, source=source),
            data_type=lineage_values.Datatype("FLOAT"),
        ),
        right=lineage_functions.data_type.Cast(
            source=lineage_functions.aggregate.Count(source=source),
            data_type=lineage_values.Datatype("FLOAT"),
        ),
    )
