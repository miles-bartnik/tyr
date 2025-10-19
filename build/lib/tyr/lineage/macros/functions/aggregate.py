from ... import core as lineage
from ... import values as lineage_values
from ... import functions as lineage_functions


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
