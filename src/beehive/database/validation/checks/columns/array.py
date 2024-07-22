from ..core import Check
from .....beeswax import lineage


def array_length(
    source: lineage.core._Column,
    scope: str,
    granularity: lineage.values.Interval = None,
):
    return Check(
        name="ArrayLength",
        columns_checked=lineage.core.ColumnList([source]),
        tables_checked=lineage.core.TableList(
            [lineage.tables.Select(source.current_table)]
        ),
        result=lineage.functions.aggregate.Count(source),
        scope=scope,
        source=lineage.tables.Select(source.current_table),
    )
