from ..core import Check
from tyr import lineage


def count(
    source: lineage.core._Column,
    scope: str,
    granularity: lineage.values.Interval = None,
):
    return Check(
        name="Count",
        columns_checked=lineage.core.ColumnList([source]),
        tables_checked=lineage.core.TableList(
            [lineage.tables.Select(source.current_table)]
        ),
        result=lineage.functions.aggregate.Count(source),
        scope=scope,
        source=lineage.tables.Select(source.current_table),
        granularity=granularity,
    )


def count_distinct(
    source: lineage.core._Column,
    scope: str,
    granularity: lineage.values.Interval = None,
):
    return Check(
        name="CountDistinct",
        columns_checked=lineage.core.ColumnList([source]),
        tables_checked=lineage.core.TableList(
            [lineage.tables.Select(source.current_table)]
        ),
        result=lineage.functions.aggregate.Count(
            source, distinct=True
        ),
        scope=scope,
        source=lineage.tables.Select(source.current_table),
        granularity=granularity,
    )


def count_null(
    source: lineage.core._Column,
    scope: str,
    granularity: lineage.values.Interval = None,
):
    return Check(
        name="CountNull",
        columns_checked=lineage.core.ColumnList([source]),
        tables_checked=lineage.core.TableList(
            [lineage.tables.Select(source.current_table)]
        ),
        result=lineage.functions.aggregate.Sum(
            source=lineage.core.CaseWhen(
                conditions=[
                    lineage.core.Condition(
                        checks=[
                            lineage.expressions.Is(
                                left=source,
                                right=lineage.values.Null(),
                            )
                        ]
                    )
                ],
                values=[lineage.values.Integer(1)],
                else_value=lineage.values.Integer(0),
            )
        ),
        scope=scope,
        source=lineage.tables.Select(source.current_table),
        granularity=granularity,
    )


def proportion_null(
    source: lineage.core._Column,
    scope: str,
    granularity: lineage.values.Interval = None,
):
    return Check(
        name="ProportionNull",
        columns_checked=lineage.core.ColumnList([source]),
        tables_checked=lineage.core.TableList(
            [lineage.tables.Select(source.current_table)]
        ),
        result=lineage.functions.math.Divide(
            left=lineage.functions.math.Divide(
                lineage.functions.data_type.Cast(
                    count_null(
                        source=source, scope=scope, granularity=granularity
                    ).result,
                    lineage.values.Datatype("FLOAT"),
                )
            ),
            right=lineage.functions.aggregate.Count(
                lineage.values.WildCard()
            ),
        ),
        scope=scope,
        source=lineage.tables.Select(source.current_table),
        granularity=granularity,
    )
