from ..core import Check
from tyr import lineage


def standard_deviation(
    source: lineage.core._Column,
    scope: str,
    granularity: lineage.values.Interval = None,
):
    return Check(
        name="StandardDeviation",
        columns_checked=lineage.core.ColumnList([source]),
        tables_checked=lineage.core.TableList(
            [lineage.tables.Select(source.current_table)]
        ),
        result=lineage.functions.aggregate.StandardDeviation(
            source
        ),
        scope=scope,
        source=lineage.tables.Select(source.current_table),
        granularity=granularity,
    )


def average(
    source: lineage.core._Column,
    scope: str,
    granularity: lineage.values.Interval = None,
):
    return Check(
        name="Average",
        columns_checked=lineage.core.ColumnList([source]),
        tables_checked=lineage.core.TableList(
            [lineage.tables.Select(source.current_table)]
        ),
        result=lineage.functions.aggregate.Average(source),
        scope=scope,
        source=lineage.tables.Select(source.current_table),
        granularity=granularity,
    )


def minimum(
    source: lineage.core._Column,
    scope: str,
    granularity: lineage.values.Interval = None,
):
    return Check(
        name="Minimum",
        columns_checked=lineage.core.ColumnList([source]),
        tables_checked=lineage.core.TableList(
            [lineage.tables.Select(source.current_table)]
        ),
        result=lineage.functions.aggregate.Minimum(source),
        scope=scope,
        source=lineage.tables.Select(source.current_table),
        granularity=granularity,
    )


def maximum(
    source: lineage.core._Column,
    scope: str,
    granularity: lineage.values.Interval = None,
):
    return Check(
        name="Maximum",
        columns_checked=lineage.core.ColumnList([source]),
        tables_checked=lineage.core.TableList(
            [lineage.tables.Select(source.current_table)]
        ),
        result=lineage.functions.aggregate.Maximum(source),
        scope=scope,
        source=lineage.tables.Select(source.current_table),
        granularity=granularity,
    )


def interdecile_range(
    source: lineage.core._Column,
    scope: str,
    granularity: lineage.values.Interval = None,
):
    return Check(
        name="InterdecileRange",
        columns_checked=lineage.core.ColumnList([source]),
        tables_checked=lineage.core.TableList(
            [lineage.tables.Select(source.current_table)]
        ),
        result=lineage.functions.aggregate.Maximum(source),
        scope=scope,
        source=lineage.tables.Select(source.current_table),
        granularity=granularity,
    )
