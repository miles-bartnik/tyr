from ....lineage import core as lineage
from ....lineage import columns as lineage_columns
from ....lineage import expressions as lineage_expressions
from ....lineage import functions as lineage_functions
from ....lineage import values as lineage_values
from ....lineage import tables as lineage_tables
from ....lineage import joins as lineage_combinations
from ....lineage import operators as lineage_operators


def luminance(source):
    Y = lineage_functions.math.Add(
        lineage_functions.math.Add(
            lineage_functions.math.Multiply(
                lineage_values.Float(0.2126),
                lineage.CaseWhen(
                    conditions=[
                        lineage.Condition(
                            checks=[
                                lineage_expressions.LessThanOrEqual(
                                    lineage_functions.math.Divide(
                                        lineage_functions.array.ListExtract(
                                            source=source,
                                            elements=[lineage_values.Integer(1)],
                                        ),
                                        lineage_values.Integer(255),
                                    )
                                )
                            ]
                        )
                    ],
                    values=[
                        lineage_functions.math.Divide(
                            source, lineage_values.Float(12.92)
                        )
                    ],
                    else_value=lineage_functions.math.Exponent(
                        lineage_functions.math.Divide(
                            lineage_functions.math.Add(
                                lineage_functions.array.ListExtract(
                                    source=source,
                                    elements=[lineage_values.Integer(1)],
                                ),
                                lineage_values.Float(0.055),
                            ),
                            lineage_values.Float(1.055),
                        ),
                        lineage_values.Float(2.4),
                    ),
                ),
            ),
            lineage_functions.math.Multiply(
                lineage_values.Float(0.7152),
                lineage.CaseWhen(
                    conditions=[
                        lineage.Condition(
                            checks=[
                                lineage_expressions.LessThanOrEqual(
                                    lineage_functions.math.Divide(
                                        lineage_functions.array.ListExtract(
                                            source=source,
                                            elements=[lineage_values.Integer(2)],
                                        ),
                                        lineage_values.Integer(255),
                                    )
                                )
                            ]
                        )
                    ],
                    values=[
                        lineage_functions.math.Divide(
                            source, lineage_values.Float(12.92)
                        )
                    ],
                    else_value=lineage_functions.math.Exponent(
                        lineage_functions.math.Divide(
                            lineage_functions.math.Add(
                                lineage_functions.array.ListExtract(
                                    source=source,
                                    elements=[lineage_values.Integer(2)],
                                ),
                                lineage_values.Float(0.055),
                            ),
                            lineage_values.Float(1.055),
                        ),
                        lineage_values.Float(2.4),
                    ),
                ),
            ),
        ),
        lineage_functions.math.Multiply(
            lineage_values.Float(0.0722),
            lineage.CaseWhen(
                conditions=[
                    lineage.Condition(
                        checks=[
                            lineage_expressions.LessThanOrEqual(
                                lineage_functions.math.Divide(
                                    lineage_functions.array.ListExtract(
                                        source=source,
                                        elements=[lineage_values.Integer(3)],
                                    ),
                                    lineage_values.Integer(255),
                                )
                            )
                        ]
                    )
                ],
                values=[
                    lineage_functions.math.Divide(source, lineage_values.Float(12.92))
                ],
                else_value=lineage_functions.math.Exponent(
                    lineage_functions.math.Divide(
                        lineage_functions.math.Add(
                            lineage_functions.array.ListExtract(
                                source=source,
                                elements=[lineage_values.Integer(3)],
                            ),
                            lineage_values.Float(0.055),
                        ),
                        lineage_values.Float(1.055),
                    ),
                    lineage_values.Float(2.4),
                ),
            ),
        ),
    )

    return lineage.CaseWhen(
        conditions=[
            lineage.Condition(
                checks=[
                    lineage_expressions.LessThanOrEqual(
                        Y, lineage_values.Float(0.008856)
                    )
                ]
            )
        ],
        values=[lineage_functions.math.Multiply(Y, lineage_values.Float(903.3))],
        else_value=lineage_functions.math.Subtract(
            lineage_functions.math.Multiply(
                lineage_functions.math.Exponent(Y, lineage_values.Float(1 / 3)),
                lineage_values.Float(116),
            ),
            lineage_values.Float(16),
        ),
    )
