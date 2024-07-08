from ..core import Check
from .....beeswax import lineage


def count_is_email(
    source: lineage.core._Column,
    scope: str,
    granularity: lineage.values.Interval = None,
):
    return Check(
        name="IsEmail",
        columns_checked=lineage.core.ColumnList([source]),
        tables_checked=lineage.core.TableList(
            [lineage.tables.Select(source.current_table)]
        ),
        result=lineage.functions.Sum(
            lineage.core.CaseWhen(
                conditions=[
                    lineage.core.Condition(
                        checks=[
                            lineage.expressions.Is(
                                left=lineage.functions.RegExpMatch(
                                    source,
                                    r"\A[a-z0-9!#$%&'*+/=?^_`{|}~-]+(?:\.[a-z0-9!#$%&'*+/=?^_`{|}~-]+)*@(?:[a-z0-9](?:[a-z0-9-]*[a-z0-9])?\.)+[a-z0-9](?:[a-z0-9-]*[a-z0-9])?\z",
                                ),
                                right=lineage.values.Boolean(True),
                            )
                        ]
                    )
                ],
                values=[lineage.values.Integer(1)],
                else_value=[lineage.values.Integer(0)],
            )
        ),
        scope=scope,
        source=lineage.tables.Select(source.current_table),
        granularity=granularity,
    )


def proportion_is_email(
    source: lineage.core._Column,
    scope: str,
    granularity: lineage.values.Interval = None,
):
    return Check(
        name="IsEmail",
        columns_checked=lineage.core.ColumnList([source]),
        tables_checked=lineage.core.TableList(
            [lineage.tables.Select(source.current_table)]
        ),
        result=lineage.functions.Divide(
            left=lineage.functions.Cast(
                count_is_email(
                    source=source, scope=scope, granularity=granularity
                ).result,
                lineage.values.Datatype("FLOAT"),
            ),
            right=lineage.functions.Cast(
                lineage.functions.Count(lineage.values.WildCard()),
                lineage.values.Datatype("FLOAT"),
            ),
        ),
        scope=scope,
        source=lineage.tables.Select(source.current_table),
        granularity=granularity,
    )
