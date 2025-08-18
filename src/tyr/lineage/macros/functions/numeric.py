from ... import core as lineage
from ... import expressions as lineage_expressions
from ... import values as lineage_values
from ... import functions as lineage_functions


def significant_figures(source, value):
    return lineage.CaseWhen(
        conditions=[
            lineage.Condition(
                checks=[
                    lineage_expressions.Is(
                        left=lineage_functions.math.AbsoluteValue(source),
                        right=lineage_values.Null(),
                    ),
                ]
            ),
            lineage.Condition(
                checks=[
                    lineage_expressions.NotEqual(
                        left=lineage_functions.math.AbsoluteValue(source),
                        right=lineage_values.Float(0),
                    ),
                ],
            ),
        ],
        values=[
            lineage_values.Null(data_type=lineage_values.Datatype("INTEGER")),
            lineage_functions.math.Round(
                source=source,
                precision=tyr.lineage.functions.data_type.Cast(
                    lineage_functions.math.Subtract(
                        lineage_functions.math.Subtract(
                            value, lineage_values.Integer(1)
                        ),
                        lineage_functions.math.Floor(
                            lineage_functions.math.Log10(
                                lineage_functions.math.AbsoluteValue(source),
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
    return lineage_functions.math.Divide(
        lineage_functions.math.Log10(source),
        lineage_functions.math.Log10(base),
    )
