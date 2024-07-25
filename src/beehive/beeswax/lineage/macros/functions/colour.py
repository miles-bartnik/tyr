from ....lineage import core as lineage
from ....lineage import columns as lineage_columns
from ....lineage import expressions as lineage_expressions
from ....lineage import functions as lineage_functions
from ....lineage import values as lineage_values
from ....lineage import tables as lineage_tables
from ....lineage import joins as lineage_combinations
from ....lineage import operators as lineage_operators


def hex_to_rgb(source):
    return lineage_values.List(
        [
            lineage_functions.data_type.Cast(
                lineage_functions.string.Concatenate(
                    [
                        lineage_values.Varchar("0x"),
                        lineage_functions.string.StringExtract(
                            source=lineage_functions.string.RegExpReplace(
                                source=lineage_functions.string.RegExpReplace(
                                    source,
                                    lineage_values.Varchar("#"),
                                    lineage_values.Varchar(""),
                                ),
                                regex=lineage_values.Varchar("0x"),
                                value=lineage_values.Varchar(""),
                            ),
                            elements=lineage_values.List(
                                [lineage_values.Integer(i) for i in elements]
                            ),
                        ),
                    ]
                ),
                data_type=lineage_values.Datatype("INTEGER"),
            )
            for elements in [[1, 2], [3, 4], [5, 6]]
        ]
    )


def rgb_to_hex(source):
    return lineage_functions.string.Concatenate(
        [
            lineage_values.Varchar("#"),
            lineage.CaseWhen(
                conditions=[
                    lineage.Condition(
                        checks=[
                            lineage_expressions.Equal(
                                lineage_functions.string.Length(
                                    lineage_functions.data_type.IntegerToHex(
                                        lineage_functions.data_type.Cast(
                                            lineage_functions.array.ListExtract(
                                                source,
                                                lineage_values.List(
                                                    [lineage_values.Integer(1)]
                                                ),
                                            ),
                                            lineage_values.Datatype("INTEGER"),
                                        )
                                    )
                                ),
                                lineage_values.Integer(1),
                            )
                        ]
                    )
                ],
                values=[
                    lineage_functions.string.Concatenate(
                        [
                            lineage_values.Varchar("0"),
                            lineage_functions.data_type.IntegerToHex(
                                lineage_functions.data_type.Cast(
                                    lineage_functions.array.ListExtract(
                                        source,
                                        lineage_values.List(
                                            [lineage_values.Integer(1)]
                                        ),
                                    ),
                                    lineage_values.Datatype("INTEGER"),
                                )
                            ),
                        ],
                        join_string="",
                    )
                ],
                else_value=lineage_functions.data_type.IntegerToHex(
                    lineage_functions.data_type.Cast(
                        lineage_functions.array.ListExtract(
                            source,
                            lineage_values.List([lineage_values.Integer(1)]),
                        ),
                        lineage_values.Datatype("INTEGER"),
                    )
                ),
            ),
            lineage.CaseWhen(
                conditions=[
                    lineage.Condition(
                        checks=[
                            lineage_expressions.Equal(
                                lineage_functions.string.Length(
                                    lineage_functions.data_type.IntegerToHex(
                                        lineage_functions.data_type.Cast(
                                            lineage_functions.array.ListExtract(
                                                source,
                                                lineage_values.List(
                                                    [lineage_values.Integer(2)]
                                                ),
                                            ),
                                            lineage_values.Datatype("INTEGER"),
                                        )
                                    )
                                ),
                                lineage_values.Integer(1),
                            )
                        ]
                    )
                ],
                values=[
                    lineage_functions.string.Concatenate(
                        [
                            lineage_values.Varchar("0"),
                            lineage_functions.data_type.IntegerToHex(
                                lineage_functions.data_type.Cast(
                                    lineage_functions.array.ListExtract(
                                        source,
                                        lineage_values.List(
                                            [lineage_values.Integer(2)]
                                        ),
                                    ),
                                    lineage_values.Datatype("INTEGER"),
                                )
                            ),
                        ],
                        join_string="",
                    )
                ],
                else_value=lineage_functions.data_type.IntegerToHex(
                    lineage_functions.data_type.Cast(
                        lineage_functions.array.ListExtract(
                            source,
                            lineage_values.List([lineage_values.Integer(2)]),
                        ),
                        lineage_values.Datatype("INTEGER"),
                    )
                ),
            ),
            lineage.CaseWhen(
                conditions=[
                    lineage.Condition(
                        checks=[
                            lineage_expressions.Equal(
                                lineage_functions.string.Length(
                                    lineage_functions.data_type.IntegerToHex(
                                        lineage_functions.data_type.Cast(
                                            lineage_functions.array.ListExtract(
                                                source,
                                                lineage_values.List(
                                                    [lineage_values.Integer(3)]
                                                ),
                                            ),
                                            lineage_values.Datatype("INTEGER"),
                                        )
                                    )
                                ),
                                lineage_values.Integer(1),
                            )
                        ]
                    )
                ],
                values=[
                    lineage_functions.string.Concatenate(
                        [
                            lineage_values.Varchar("0"),
                            lineage_functions.data_type.IntegerToHex(
                                lineage_functions.data_type.Cast(
                                    lineage_functions.array.ListExtract(
                                        source,
                                        lineage_values.List(
                                            [lineage_values.Integer(3)]
                                        ),
                                    ),
                                    lineage_values.Datatype("INTEGER"),
                                )
                            ),
                        ],
                        join_string="",
                    )
                ],
                else_value=lineage_functions.data_type.IntegerToHex(
                    lineage_functions.data_type.Cast(
                        lineage_functions.array.ListExtract(
                            source,
                            lineage_values.List([lineage_values.Integer(3)]),
                        ),
                        lineage_values.Datatype("INTEGER"),
                    )
                ),
            ),
        ]
    )


def rgb_to_hsv(source):
    Rp = lineage_functions.math.Divide(
        lineage_functions.data_type.Cast(
            lineage_functions.array.ListExtract(
                source, lineage_values.List([lineage_values.Integer(1)])
            ),
            lineage_values.Datatype("FLOAT"),
        ),
        lineage_values.Float(255),
    )

    Gp = lineage_functions.math.Divide(
        lineage_functions.data_type.Cast(
            lineage_functions.array.ListExtract(
                source, lineage_values.List([lineage_values.Integer(2)])
            ),
            lineage_values.Datatype("FLOAT"),
        ),
        lineage_values.Float(255),
    )

    Bp = lineage_functions.math.Divide(
        lineage_functions.data_type.Cast(
            lineage_functions.array.ListExtract(
                source, lineage_values.List([lineage_values.Integer(3)])
            ),
            lineage_values.Datatype("FLOAT"),
        ),
        lineage_values.Float(255),
    )

    Cmax = lineage_functions.array.Maximum(lineage_values.List([Rp, Gp, Bp]))
    Cmin = lineage_functions.array.Minimum(lineage_values.List([Rp, Gp, Bp]))
    delta = lineage_functions.math.Subtract(Cmax, Cmin)

    H = lineage_functions.math.Divide(
        lineage_functions.data_type.Cast(
            lineage.CaseWhen(
                conditions=[
                    lineage.Condition(
                        checks=[
                            lineage_expressions.Equal(delta, lineage_values.Integer(0))
                        ]
                    ),
                    lineage.Condition(checks=[lineage_expressions.Equal(Cmax, Rp)]),
                    lineage.Condition(checks=[lineage_expressions.Equal(Cmax, Gp)]),
                    lineage.Condition(checks=[lineage_expressions.Equal(Cmax, Bp)]),
                ],
                values=[
                    lineage_values.Float(0),
                    lineage_functions.math.Multiply(
                        lineage_values.Float(60),
                        lineage_functions.math.Mod(
                            lineage_functions.math.Divide(
                                lineage_functions.math.Subtract(Gp, Bp), delta
                            ),
                            lineage_values.Float(6),
                        ),
                    ),
                    lineage_functions.math.Multiply(
                        lineage_values.Float(60),
                        lineage_functions.math.Add(
                            lineage_functions.math.Divide(
                                lineage_functions.math.Subtract(Bp, Rp), delta
                            ),
                            lineage_values.Float(2),
                        ),
                    ),
                    lineage_functions.math.Multiply(
                        lineage_values.Float(60),
                        lineage_functions.math.Add(
                            lineage_functions.math.Divide(
                                lineage_functions.math.Subtract(Rp, Gp), delta
                            ),
                            lineage_values.Float(4),
                        ),
                    ),
                ],
            ),
            lineage_values.Datatype("FLOAT"),
        ),
        lineage_values.Float(360),
    )

    S = lineage_functions.data_type.Cast(
        lineage.CaseWhen(
            conditions=[
                lineage.Condition(
                    checks=[lineage_expressions.Equal(Cmax, lineage_values.Float(0))]
                )
            ],
            values=[lineage_values.Float(0)],
            else_value=lineage_functions.math.Divide(delta, Cmax),
        ),
        lineage_values.Datatype("FLOAT"),
    )

    V = lineage_functions.data_type.Cast(Cmax, lineage_values.Datatype("FLOAT"))

    return lineage_values.List([H, S, V])


def hsv_to_rgb(source):
    H = lineage_functions.array.ListExtract(
        source, elements=lineage_values.List([lineage_values.Integer(1)])
    )

    S = lineage_functions.array.ListExtract(
        source, elements=lineage_values.List([lineage_values.Integer(2)])
    )

    V = lineage_functions.array.ListExtract(
        source, elements=lineage_values.List([lineage_values.Integer(3)])
    )

    Hp = lineage_functions.math.Multiply(H, lineage_values.Float(6))

    C = lineage_functions.math.Multiply(V, S)

    X = lineage_functions.math.Multiply(
        C,
        lineage_functions.math.Subtract(
            lineage_values.Float(1),
            lineage_functions.math.AbsoluteValue(
                lineage_functions.math.Subtract(
                    lineage_functions.math.Mod(
                        Hp,
                        lineage_values.Float(2),
                    ),
                    lineage_values.Float(1),
                )
            ),
        ),
    )

    m = lineage_functions.math.Subtract(V, C)

    conditions = [
        lineage.Condition(
            checks=[
                lineage_expressions.GreaterThanOrEqual(Hp, lineage_values.Float(0)),
                lineage_expressions.LessThan(
                    Hp,
                    lineage_values.Float(1),
                ),
            ],
            link_operators=[lineage_operators.And()],
        )
    ] + [
        lineage.Condition(
            checks=[
                lineage_expressions.GreaterThanOrEqual(
                    Hp,
                    lineage_values.Float(i),
                ),
                lineage_expressions.LessThan(
                    Hp,
                    lineage_values.Float(i + 1),
                ),
            ],
            link_operators=[lineage_operators.And()],
        )
        for i in range(1, 6)
    ]

    Rp = lineage.CaseWhen(
        conditions=conditions,
        values=[
            C,
            X,
            lineage_values.Float(0),
            lineage_values.Float(0),
            X,
            C,
        ],
    )

    Gp = lineage.CaseWhen(
        conditions=conditions,
        values=[
            X,
            C,
            C,
            X,
            lineage_values.Float(0),
            lineage_values.Float(0),
        ],
    )

    Bp = lineage.CaseWhen(
        conditions=conditions,
        values=[
            lineage_values.Float(0),
            lineage_values.Float(0),
            X,
            C,
            C,
            X,
        ],
    )

    return lineage_values.List(
        [
            lineage_functions.data_type.Cast(
                lineage_functions.math.Multiply(
                    lineage_functions.math.Add(Rp, m), lineage_values.Float(255)
                ),
                data_type=lineage_values.Datatype("INTEGER"),
            ),
            lineage_functions.data_type.Cast(
                lineage_functions.math.Multiply(
                    lineage_functions.math.Add(Gp, m), lineage_values.Float(255)
                ),
                data_type=lineage_values.Datatype("INTEGER"),
            ),
            lineage_functions.data_type.Cast(
                lineage_functions.math.Multiply(
                    lineage_functions.math.Add(Bp, m), lineage_values.Float(255)
                ),
                data_type=lineage_values.Datatype("INTEGER"),
            ),
        ]
    )
