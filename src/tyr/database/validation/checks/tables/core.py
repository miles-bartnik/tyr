import units

from ..core import Check
from tyr import lineage

def primary_key_completeness(
    source: lineage.core._Table, scope: str, granularity: lineage.values.Interval = None
):
    result = (
        lineage.macros.functions.aggregate.distinct_proportion(
            source=lineage.values.Tuple(
                values=lineage.macros.columns.select_all(source.primary_key).list_all()
            )
        )
    )

    # success = lineage.tables.Core(
    #     name="primary_key_examples",
    #     source=table,
    #     columns=table.columns,
    #     where_condition=lineage.core.Condition(checks=)
    # )

    return Check(
        name="PrimaryKeyCompleteness",
        tables_checked=lineage.core.TableList([source]),
        columns_checked=source.primary_key,
        source=source,
        result=result,
        scope=scope,
        granularity=granularity,
    )


def event_time_standard_deviation(
    source: lineage.core._Table, scope: str, granularity: lineage.values.Interval = None
):
    if source.event_time.data_type.value in ["TIMESTAMP"]:
        result = lineage.functions.data_type.ToInterval(
            source=lineage.functions.data_type.Cast(
                source=lineage.functions.aggregate.StandardDeviation(
                    source=lineage.functions.datetime.TimestampToEpochMS(
                        lineage.columns.Select(source.event_time)
                    )
                ),
                data_type=lineage.values.Datatype("INTEGER"),
            ),
            unit=units.core.Unit("ms^1"),
        )

    elif source.event_time.data_type.value in ["INTEGER", "FLOAT"]:
        result = lineage.functions.aggregate.StandardDeviation(
            source=lineage.columns.Select(source.event_time)
        )
    else:
        raise ValueError(
            rf"Invalid data_type {source.event_time.data_type.value} for event_time"
        )

    return Check(
        name="EventTimeStandardDeviation",
        tables_checked=lineage.core.TableList([source]),
        columns_checked=lineage.core.ColumnList([source.event_time]),
        result=result,
        scope=scope,
        source=source,
        granularity=granularity,
    )
