from ....lineage import core as lineage
from ....lineage import columns as lineage_columns
from ....lineage import expressions as lineage_expressions
from ....lineage import functions as lineage_functions
from ....lineage import values as lineage_values
from ....lineage import tables as lineage_tables
from ....lineage import aggregates as lineage_aggregates
from ....lineage import combinations as lineage_combinations
from ....lineage import operators as lineage_operators


def significant_figures(source, value):
    return lineage.CaseWhen(
        conditions=[
            lineage.Condition(
                checks=[
                    lineage_expressions.Is(
                        left=lineage_functions.AbsoluteValue(source),
                        right=lineage_values.Null(),
                    ),
                ]
            ),
            lineage.Condition(
                checks=[
                    lineage_expressions.NotEqual(
                        left=lineage_functions.AbsoluteValue(source),
                        right=lineage_values.Float(0),
                    ),
                ],
            ),
        ],
        values=[
            lineage_values.Null(data_type=lineage_values.Datatype("INTEGER")),
            lineage_functions.Round(
                source=source,
                precision=lineage_functions.Cast(
                    lineage_functions.Subtract(
                        lineage_functions.Subtract(value, lineage_values.Integer(1)),
                        lineage_functions.Floor(
                            lineage_functions.Log10(
                                lineage_functions.AbsoluteValue(source),
                            )
                        ),
                    ),
                    lineage_values.Datatype("INTEGER"),
                ),
            ),
        ],
        else_value=lineage_values.Integer(0),
    )


def log_ab(source, base):
    return lineage_functions.Divide(
        lineage_functions.Log10(source), lineage_functions.Log10(base)
    )
