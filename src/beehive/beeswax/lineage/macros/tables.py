import pandas as pd

from ...lineage import core as lineage
from ...lineage import values as lineage_values
from ...lineage import functions as lineage_functions
from ...lineage import columns as lineage_columns
from ...lineage import aggregates as lineage_aggregates
from ...lineage import expressions as lineage_expressions
from ...lineage import operators as lineage_operators
from ...lineage import tables as lineage_tables
from ...lineage import combinations as lineage_combinations
from .functions import interpolate

import numpy as np


def event_time_interval_transform(
    source,
    interval,
    max_n_intervals: int = 2047,
    interpolation_function=interpolate.linear,
    columns_to_interpolate: lineage.ColumnList = lineage.ColumnList([]),
):
    if not (source.event_time and source.primary_key):
        raise ValueError(rf"""Source table has no event_time or primary_key defined""")
    elif not source.event_time:
        raise ValueError(rf"""Source table has no event_time defined""")
    elif not source.primary_key:
        raise ValueError(rf"""Source table has no primary_key defined""")

    if max_n_intervals > 2047:
        raise ValueError("Maximum date_array size cannot exceed 2047")

    if type(source) is lineage_tables.Core:
        source = lineage_tables.Subquery(source)

    base = lineage_tables.Core(
        name=rf"{source.name}_base",
        source=source,
        columns=lineage.ColumnList(
            lineage_columns.select_all(source.columns).list_all()
            + [
                lineage_columns.Expand(
                    source=lineage_functions.Lag(
                        source=lineage_columns.Select(column),
                        partition_by=lineage.PartitionBy(
                            lineage.ColumnList(
                                [
                                    lineage_columns.Select(column)
                                    for column in source.static_primary_key.list_all()
                                ]
                            )
                        ),
                        order_by=lineage.OrderBy(
                            lineage.ColumnList(
                                [lineage_columns.Select(source.event_time)]
                            ),
                            how=[lineage_operators.Descending()],
                        ),
                    ),
                    name=rf"lag_{column.name}",
                    var_type="numeric",
                )
                for column in source.columns[columns_to_interpolate.list_names()]
            ]
        ),
        event_time=lineage_columns.Select(source.event_time),
        primary_key=lineage.ColumnList(
            [lineage_columns.Select(column) for column in source.primary_key.list_all()]
        ),
    )

    date_arrays = lineage_tables.Core(
        name=rf"{source.name}_date_arrays",
        ctes=lineage.TableList([base]),
        source=lineage_tables.Select(base),
        columns=lineage.ColumnList(
            [
                lineage_columns.Select(column)
                for column in base.columns[base.primary_key.list_names()]
            ]
            + [
                lineage_columns.Expand(
                    source=lineage_functions.Lag(
                        source=lineage_columns.Select(base.event_time),
                        order_by=lineage.OrderBy(
                            columns=lineage.ColumnList(
                                [lineage_columns.Select(base.event_time)]
                            ),
                            how=[lineage_operators.Descending()],
                        ),
                        partition_by=lineage.PartitionBy(
                            lineage.ColumnList(
                                [
                                    lineage_columns.Select(column)
                                    for column in base.columns[
                                        base.static_primary_key.list_names()
                                    ]
                                ]
                            )
                        ),
                    ),
                    name=rf"lag_{base.event_time.name}",
                ),
                lineage_columns.Expand(
                    source=lineage_functions.DateBin(
                        source=lineage_columns.Select(base.event_time),
                        interval=interval,
                    ),
                    name=rf"trunc_{base.event_time.name}",
                ),
                lineage_columns.Expand(
                    source=lineage_functions.DateBin(
                        source=lineage_functions.Lag(
                            source=lineage_columns.Select(base.event_time),
                            order_by=lineage.OrderBy(
                                columns=lineage.ColumnList(
                                    [lineage_columns.Select(base.event_time)]
                                ),
                                how=[lineage_operators.Descending()],
                            ),
                            partition_by=lineage.PartitionBy(
                                lineage.ColumnList(
                                    [
                                        lineage_columns.Select(column)
                                        for column in base.columns[
                                            base.static_primary_key.list_names()
                                        ]
                                    ]
                                )
                            ),
                        ),
                        interval=interval,
                    ),
                    name=rf"trunc_lag_{base.event_time.name}",
                ),
                lineage_columns.Expand(
                    source=lineage.CaseWhen(
                        conditions=[
                            lineage.Condition(
                                checks=[
                                    lineage_expressions.LessThan(
                                        left=lineage_functions.Multiply(
                                            left=lineage_values.Interval(
                                                value=1,
                                                unit=interval.unit,
                                            ),
                                            right=lineage_functions.DateDiff(
                                                start=lineage_functions.DateBin(
                                                    source=lineage_columns.Select(
                                                        base.event_time
                                                    ),
                                                    interval=interval,
                                                ),
                                                end=lineage_functions.Lag(
                                                    source=lineage_columns.Select(
                                                        base.event_time
                                                    ),
                                                    order_by=lineage.OrderBy(
                                                        columns=lineage.ColumnList(
                                                            [
                                                                lineage_columns.Select(
                                                                    base.event_time
                                                                )
                                                            ]
                                                        ),
                                                        how=[
                                                            lineage_operators.Descending()
                                                        ],
                                                    ),
                                                    partition_by=lineage.PartitionBy(
                                                        lineage.ColumnList(
                                                            [
                                                                lineage_columns.Select(
                                                                    column
                                                                )
                                                                for column in base.columns[
                                                                    base.static_primary_key.list_names()
                                                                ]
                                                            ]
                                                        )
                                                    ),
                                                ),
                                                unit=interval.unit,
                                            ),
                                        ),
                                        right=lineage_functions.Multiply(
                                            left=lineage_values.Integer(
                                                max_n_intervals
                                            ),
                                            right=interval,
                                        ),
                                    ),
                                    lineage_expressions.GreaterThan(
                                        left=lineage_functions.Multiply(
                                            left=lineage_values.Interval(
                                                value=1,
                                                unit=interval.unit,
                                            ),
                                            right=lineage_functions.DateDiff(
                                                start=lineage_functions.DateBin(
                                                    source=lineage_columns.Select(
                                                        base.event_time
                                                    ),
                                                    interval=interval,
                                                ),
                                                end=lineage_functions.Lag(
                                                    source=lineage_columns.Select(
                                                        base.event_time
                                                    ),
                                                    order_by=lineage.OrderBy(
                                                        columns=lineage.ColumnList(
                                                            [
                                                                lineage_columns.Select(
                                                                    base.event_time
                                                                )
                                                            ]
                                                        ),
                                                        how=[
                                                            lineage_operators.Descending()
                                                        ],
                                                    ),
                                                    partition_by=lineage.PartitionBy(
                                                        lineage.ColumnList(
                                                            [
                                                                lineage_columns.Select(
                                                                    column
                                                                )
                                                                for column in base.columns[
                                                                    base.static_primary_key.list_names()
                                                                ]
                                                            ]
                                                        )
                                                    ),
                                                ),
                                                unit=interval.unit,
                                            ),
                                        ),
                                        right=interval,
                                    ),
                                ],
                                link_operators=[lineage_operators.And()],
                            )
                        ],
                        values=[
                            lineage_functions.Range(
                                start=lineage_functions.DateBin(
                                    source=lineage_columns.Select(base.event_time),
                                    interval=interval,
                                ),
                                end=lineage_functions.Lag(
                                    source=lineage_columns.Select(base.event_time),
                                    order_by=lineage.OrderBy(
                                        columns=lineage.ColumnList(
                                            [lineage_columns.Select(base.event_time)]
                                        ),
                                        how=[lineage_operators.Descending()],
                                    ),
                                    partition_by=lineage.PartitionBy(
                                        lineage.ColumnList(
                                            [
                                                lineage_columns.Select(column)
                                                for column in base.columns[
                                                    base.static_primary_key.list_names()
                                                ]
                                            ]
                                        )
                                    ),
                                ),
                                interval=interval,
                            ),
                        ],
                        else_value=lineage_values.List(
                            values=[
                                lineage_functions.DateBin(
                                    source=lineage_columns.Select(base.event_time),
                                    interval=interval,
                                )
                            ]
                        ),
                    ),
                    name="date_array",
                ),
            ]
        ),
        primary_key=lineage.ColumnList(
            [lineage_columns.Select(column) for column in base.primary_key.list_all()]
        ),
        event_time=lineage_columns.Select(base.event_time),
    )

    unnested_arrays = lineage_tables.Core(
        name=rf"{source.name}_unnested_arrays",
        ctes=lineage.TableList([base, date_arrays]),
        source=lineage_tables.Select(date_arrays),
        columns=lineage.ColumnList(
            [
                lineage_columns.Select(column)
                for column in date_arrays.primary_key.list_all()
            ]
            + [
                lineage_columns.Expand(
                    source=lineage_functions.Unnest(
                        source=lineage_columns.Select(date_arrays.columns.date_array)
                    ),
                    name=rf"trunc_{date_arrays.event_time.name}",
                )
            ]
        ),
        primary_key=lineage.ColumnList(
            [
                lineage_columns.Select(column)
                for column in date_arrays.primary_key.list_all()
            ]
        ),
        event_time=lineage_columns.Select(date_arrays.event_time),
    )

    row_number_assigned = lineage_tables.Core(
        name=rf"{source.name}_row_number_assigned",
        ctes=lineage.TableList([base, date_arrays, unnested_arrays]),
        source=lineage_combinations.Join(
            join_expression=lineage_expressions.LeftJoin(
                left=lineage_tables.Select(unnested_arrays),
                right=lineage_tables.Select(base),
            ),
            condition=lineage.Condition(
                checks=[
                    lineage_expressions.Equal(
                        left=lineage_columns.Select(column),
                        right=lineage_columns.Select(
                            unnested_arrays.columns[column.name]
                        ),
                    )
                    for column in base.primary_key.list_all()
                ],
                link_operators=[
                    lineage_operators.And()
                    for i in range(len(base.primary_key.list_all()) - 1)
                ],
            ),
        ),
        columns=lineage.ColumnList(
            [
                lineage_columns.Select(column)
                for column in unnested_arrays.columns.list_all()
            ]
            + [
                lineage_columns.Expand(
                    source=lineage_functions.RowNumber(
                        partition_by=lineage.PartitionBy(
                            lineage.ColumnList(
                                [
                                    lineage_columns.Select(column)
                                    for column in unnested_arrays.static_primary_key.list_all()
                                ]
                                + [
                                    lineage_columns.Select(
                                        unnested_arrays.columns[
                                            rf"trunc_{unnested_arrays.event_time.name}"
                                        ]
                                    )
                                ]
                            )
                        ),
                        order_by=lineage.OrderBy(
                            columns=lineage.ColumnList(
                                [lineage_columns.Select(unnested_arrays.event_time)]
                            ),
                            how=[lineage_operators.Descending()],
                        ),
                    ),
                    name="row_num",
                )
            ]
        ),
        primary_key=lineage.ColumnList(
            [
                lineage_columns.Select(column)
                for column in unnested_arrays.primary_key.list_all()
            ]
        ),
        event_time=lineage_columns.Select(unnested_arrays.event_time),
    )

    max_row_number = lineage_tables.Core(
        name=rf"{source.name}_max_row_number",
        source=lineage_tables.Select(row_number_assigned),
        ctes=lineage.TableList(
            [
                base,
                date_arrays,
                unnested_arrays,
                row_number_assigned,
            ]
        ),
        primary_key=lineage.ColumnList(
            [
                lineage_columns.Select(column)
                for column in row_number_assigned.primary_key.list_all()
            ]
        ),
        event_time=lineage_columns.Select(row_number_assigned.event_time),
        columns=lineage.ColumnList(
            [
                lineage_columns.Select(column)
                for column in row_number_assigned.primary_key.list_all()
            ]
            + [
                lineage_columns.Expand(
                    source=lineage_aggregates.Maximum(
                        source=lineage_columns.Select(
                            row_number_assigned.columns.row_num
                        )
                    ),
                    name="max_row_num",
                )
            ]
        ),
        group_by=True,
    )

    transformed = lineage_tables.Core(
        name=rf"{source.name}_transformed",
        ctes=lineage.TableList(
            [
                base,
                date_arrays,
                unnested_arrays,
                row_number_assigned,
                max_row_number,
            ]
        ),
        source=lineage_combinations.JoinList(
            [
                lineage_combinations.Join(
                    join_expression=lineage_expressions.LeftJoin(
                        left=lineage_tables.Select(base),
                        right=lineage_tables.Select(row_number_assigned),
                    ),
                    condition=lineage.Condition(
                        checks=[
                            lineage_expressions.Equal(
                                left=lineage_columns.Select(base.columns[column.name]),
                                right=lineage_columns.Select(
                                    row_number_assigned.columns[column.name]
                                ),
                            )
                            for column in base.primary_key.list_all()
                        ],
                        link_operators=[
                            lineage_operators.And()
                            for i in range(len(base.primary_key.list_all()) - 1)
                        ],
                    ),
                ),
                lineage_combinations.Join(
                    join_expression=lineage_expressions.LeftJoin(
                        left=lineage_tables.Select(row_number_assigned),
                        right=lineage_tables.Select(max_row_number),
                    ),
                    condition=lineage.Condition(
                        checks=[
                            lineage_expressions.Equal(
                                left=lineage_columns.Select(
                                    row_number_assigned.columns[column.name]
                                ),
                                right=lineage_columns.Select(
                                    max_row_number.columns[column.name]
                                ),
                            )
                            for column in row_number_assigned.primary_key.list_all()
                        ],
                        link_operators=[
                            lineage_operators.And()
                            for i in range(
                                len(row_number_assigned.primary_key.list_all()) - 1
                            )
                        ],
                    ),
                ),
            ]
        ),
        columns=lineage.ColumnList(
            [
                lineage_columns.Select(column)
                for column in row_number_assigned.static_primary_key.list_all()
            ]
            + [
                lineage_columns.Select(
                    row_number_assigned.columns[
                        rf"trunc_{row_number_assigned.event_time.name}"
                    ],
                    alias=row_number_assigned.event_time.name,
                    var_type="timestamp",
                ),
            ]
            + [
                lineage_columns.Select(column)
                for column in base.columns.list_all()
                if column.name[:4] != "lag_"
                and column.name not in base.primary_key.list_names()
            ]
        ),
        where_condition=lineage.Condition(
            checks=[
                lineage_expressions.Equal(
                    left=lineage_columns.Select(row_number_assigned.columns.row_num),
                    right=lineage_values.Integer(1),
                )
            ]
        ),
        event_time=lineage_columns.Select(
            row_number_assigned.columns[
                rf"trunc_{row_number_assigned.event_time.name}"
            ],
            alias=row_number_assigned.event_time.name,
        ),
        primary_key=lineage.ColumnList(
            [
                lineage_columns.Select(column)
                for column in base.static_primary_key.list_all()
            ]
            + [
                lineage_columns.Select(
                    row_number_assigned.columns[
                        rf"trunc_{row_number_assigned.event_time.name}"
                    ],
                    alias=row_number_assigned.event_time.name,
                )
            ]
        ),
    )

    table = lineage_tables.Core(
        name=rf"{source.name}_date_dim",
        ctes=lineage.TableList(
            [
                base,
                date_arrays,
                unnested_arrays,
                row_number_assigned,
                max_row_number,
                transformed,
            ]
        ),
        source=lineage_tables.Select(transformed),
        columns=lineage.ColumnList(
            [
                lineage_columns.Select(column)
                for column in transformed.columns.list_all()
            ]
        ),
        event_time=lineage_columns.Select(transformed.event_time),
        primary_key=lineage.ColumnList(
            [
                lineage_columns.Select(column)
                for column in transformed.primary_key.list_all()
            ]
        ),
    )

    return table


def mapping(mapping_df: pd.DataFrame, columns: lineage.ColumnList):
    mapping_df = mapping_df.astype(object).where(mapping_df.notna(), None).dropna()

    return lineage_tables.Core(
        name=rf"{'_'.join(mapping_df.columns.tolist())}_mapping",
        columns=lineage.ColumnList(
            [
                lineage_columns.Expand(
                    name=column.name,
                    source=lineage_functions.Unnest(
                        lineage_values.List(
                            [
                                lineage_functions.Cast(
                                    source=lineage_values.Varchar(str(value)),
                                    data_type=column.data_type,
                                )
                                for value in mapping_df[column.name].values.tolist()
                            ]
                        )
                    ),
                )
                for column in columns.list_all()
            ]
        ),
    )


def date_vector_table(start_time, n_records: int, interval):
    return lineage_tables.Core(
        name="date_vector",
        source=lineage.RecordList(
            name="date_vector",
            records=[
                lineage.Record(
                    {
                        lineage_columns.Blank(
                            name="date",
                            var_type="timestamp",
                            data_type=lineage_values.Datatype("TIMESTAMP"),
                        ): lineage_functions.DateAdd(
                            start_time,
                            lineage_functions.Multiply(
                                lineage_values.Integer(i), interval
                            ),
                        )
                    }
                )
                for i in range(n_records)
            ],
        ),
    )


def forward_fill(source):
    source_grouped = lineage_tables.Core(
        name=rf"{source.name}_grouped",
        source=lineage_tables.Select(source),
        ctes=lineage.TableList([source]),
        columns=lineage.ColumnList(
            [lineage_columns.Select(column) for column in source.columns.list_all()]
            + [
                lineage_columns.Expand(
                    lineage_functions.Count(
                        source=lineage_columns.Select(column),
                        partition_by=lineage.PartitionBy(
                            lineage.ColumnList(
                                [
                                    lineage_columns.Select(column)
                                    for column in source.static_primary_key.list_all()
                                ]
                            )
                        ),
                        order_by=lineage.OrderBy(
                            columns=lineage.ColumnList(
                                [lineage_columns.Select(source.event_time)]
                            ),
                            how=[lineage_operators.Descending()],
                        ),
                    ),
                    name=rf"{column.name}_count",
                )
                for column in source.columns.list_all()
                if column.name not in source.primary_key.list_names()
            ]
        ),
        primary_key=lineage.ColumnList(
            [lineage_columns.Select(column) for column in source.primary_key.list_all()]
        ),
        event_time=lineage_columns.Select(source.event_time),
    )

    source_forward_filled = lineage_tables.Core(
        name=rf"{source.name}_forward_filled",
        ctes=lineage.TableList(
            [
                source,
                source_grouped,
            ]
        ),
        columns=lineage.ColumnList(
            [
                lineage_columns.Select(column)
                for column in source_grouped.primary_key.list_all()
            ]
            + [
                lineage_columns.Expand(
                    source=lineage_aggregates.Maximum(
                        lineage_columns.Select(source_grouped.columns[column.name]),
                        partition_by=lineage.PartitionBy(
                            lineage.ColumnList(
                                [
                                    lineage_columns.Select(spk)
                                    for spk in source_grouped.static_primary_key.list_all()
                                ]
                                + [
                                    lineage_columns.Select(
                                        source_grouped.columns[rf"{column.name}_count"]
                                    )
                                ]
                            ),
                        ),
                    ),
                    name=column.name,
                )
                for column in source_grouped.columns.list_all()
                if column.name
                in [
                    column
                    for column in source.columns.list_names()
                    if column not in source.primary_key.list_names()
                ]
            ]
        ),
        source=lineage_tables.Select(source_grouped),
        event_time=lineage_columns.Select(source_grouped.event_time),
        primary_key=lineage.ColumnList(
            [
                lineage_columns.Select(column)
                for column in source_grouped.primary_key.list_all()
            ]
        ),
    )

    return source_forward_filled
