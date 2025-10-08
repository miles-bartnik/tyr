from .. import core as lineage
from .. import values as lineage_values
from .. import columns as lineage_columns
from .. import expressions as lineage_expressions
from .. import operators as lineage_operators
from .. import tables as lineage_tables
from .. import joins as lineage_combinations
from .. import functions as lineage_functions
from .functions import interpolate
from .columns import staging_column_transform, select_all


def event_time_interval_transform(
    source,
    name: str,
    interval,
    max_n_intervals: int = 2047,
    interpolation_function=interpolate.linear,
    columns_to_interpolate: lineage.ColumnList = lineage.ColumnList([]),
):
    macro_group = rf"EventTimeIntervalTransform:{name} - {id(source)} - {interval.sql}"

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
            select_all(source).list_columns()
            + [
                lineage_columns.Core(
                    source=lineage_functions.window.Lag(
                        source=lineage_columns.Select(column, macro_group=macro_group),
                        partition_by=lineage.PartitionBy(
                            lineage.ColumnList(
                                [
                                    lineage_columns.Select(
                                        column, macro_group=macro_group
                                    )
                                    for column in source.static_primary_key.list_columns()
                                ]
                            )
                        ),
                        order_by=lineage.OrderBy(
                            lineage.ColumnList(
                                [
                                    lineage_columns.Select(
                                        source.event_time, macro_group=macro_group
                                    )
                                ]
                            ),
                            how=[lineage_operators.Descending(macro_group=macro_group)],
                        ),
                    ),
                    name=rf"lag_{column.name}",
                    var_type="numeric",
                    macro_group=macro_group,
                )
                for column in source.columns[columns_to_interpolate.list_names()]
            ]
        ),
        event_time=lineage_columns.Select(source.event_time, macro_group=macro_group),
        primary_key=lineage.ColumnList(
            [
                lineage_columns.Select(column, macro_group=macro_group)
                for column in source.primary_key.list_columns()
            ]
        ),
    )

    date_arrays = lineage_tables.Core(
        name=rf"{source.name}_date_arrays",
        ctes=lineage.TableList([base]),
        source=lineage_tables.Select(base, macro_group=macro_group),
        columns=lineage.ColumnList(
            [
                lineage_columns.Core(
                    name=column.name,
                    source=lineage_columns.Select(column, macro_group=macro_group),
                    macro_group=macro_group,
                )
                for column in base.columns[base.primary_key.list_names()]
            ]
            + [
                lineage_columns.Core(
                    source=lineage_functions.window.Lag(
                        source=lineage_columns.Select(
                            base.event_time, macro_group=macro_group
                        ),
                        order_by=lineage.OrderBy(
                            columns=lineage.ColumnList(
                                [
                                    lineage_columns.Select(
                                        base.event_time, macro_group=macro_group
                                    )
                                ]
                            ),
                            how=[lineage_operators.Descending(macro_group=macro_group)],
                        ),
                        partition_by=lineage.PartitionBy(
                            lineage.ColumnList(
                                [
                                    lineage_columns.Select(
                                        column, macro_group=macro_group
                                    )
                                    for column in base.columns[
                                        base.static_primary_key.list_names()
                                    ]
                                ]
                            )
                        ),
                    ),
                    name=rf"lag_{base.event_time.name}",
                ),
                lineage_columns.Core(
                    source=lineage_functions.datetime.DateBin(
                        source=lineage_columns.Select(
                            base.event_time, macro_group=macro_group
                        ),
                        interval=interval,
                        macro_group=macro_group,
                    ),
                    name=rf"trunc_{base.event_time.name}",
                    macro_group=macro_group,
                ),
                lineage_columns.Core(
                    source=lineage_functions.datetime.DateBin(
                        source=lineage_functions.window.Lag(
                            source=lineage_columns.Select(
                                base.event_time, macro_group=macro_group
                            ),
                            order_by=lineage.OrderBy(
                                columns=lineage.ColumnList(
                                    [
                                        lineage_columns.Select(
                                            base.event_time, macro_group=macro_group
                                        )
                                    ]
                                ),
                                how=[
                                    lineage_operators.Descending(
                                        macro_group=macro_group
                                    )
                                ],
                            ),
                            partition_by=lineage.PartitionBy(
                                lineage.ColumnList(
                                    [
                                        lineage_columns.Select(
                                            column, macro_group=macro_group
                                        )
                                        for column in base.columns[
                                            base.static_primary_key.list_names()
                                        ]
                                    ]
                                )
                            ),
                        ),
                        interval=interval,
                        macro_group=macro_group,
                    ),
                    name=rf"trunc_lag_{base.event_time.name}",
                    macro_group=macro_group,
                ),
                lineage_columns.Core(
                    source=lineage.CaseWhen(
                        conditions=[
                            lineage.Condition(
                                checks=[
                                    lineage_expressions.LessThan(
                                        left=lineage_functions.math.Multiply(
                                            left=lineage_values.Interval(
                                                value=1,
                                                unit=interval.unit,
                                                macro_group=macro_group,
                                            ),
                                            right=lineage_functions.datetime.DateDiff(
                                                start=lineage_functions.datetime.DateBin(
                                                    source=lineage_columns.Select(
                                                        base.event_time,
                                                        macro_group=macro_group,
                                                    ),
                                                    interval=interval,
                                                    macro_group=macro_group,
                                                ),
                                                end=lineage_functions.window.Lag(
                                                    source=lineage_columns.Select(
                                                        base.event_time,
                                                        macro_group=macro_group,
                                                    ),
                                                    order_by=lineage.OrderBy(
                                                        columns=lineage.ColumnList(
                                                            [
                                                                lineage_columns.Select(
                                                                    base.event_time,
                                                                    macro_group=macro_group,
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
                                                                    column,
                                                                    macro_group=macro_group,
                                                                )
                                                                for column in base.columns[
                                                                    base.static_primary_key.list_names()
                                                                ]
                                                            ]
                                                        )
                                                    ),
                                                    macro_group=macro_group,
                                                ),
                                                unit=interval.unit,
                                                macro_group=macro_group,
                                            ),
                                        ),
                                        right=lineage_functions.math.Multiply(
                                            left=lineage_values.Integer(
                                                max_n_intervals, macro_group=macro_group
                                            ),
                                            right=interval,
                                            macro_group=macro_group,
                                        ),
                                        macro_group=macro_group,
                                    ),
                                    lineage_expressions.GreaterThan(
                                        left=lineage_functions.math.Multiply(
                                            left=lineage_values.Interval(
                                                value=1,
                                                unit=interval.unit,
                                                macro_group=macro_group,
                                            ),
                                            right=lineage_functions.datetime.DateDiff(
                                                start=lineage_functions.datetime.DateBin(
                                                    source=lineage_columns.Select(
                                                        base.event_time,
                                                        macro_group=macro_group,
                                                    ),
                                                    interval=interval,
                                                    macro_group=macro_group,
                                                ),
                                                end=lineage_functions.window.Lag(
                                                    source=lineage_columns.Select(
                                                        base.event_time,
                                                        macro_group=macro_group,
                                                    ),
                                                    order_by=lineage.OrderBy(
                                                        columns=lineage.ColumnList(
                                                            [
                                                                lineage_columns.Select(
                                                                    base.event_time,
                                                                    macro_group=macro_group,
                                                                )
                                                            ]
                                                        ),
                                                        how=[
                                                            lineage_operators.Descending(
                                                                macro_group=macro_group
                                                            )
                                                        ],
                                                    ),
                                                    partition_by=lineage.PartitionBy(
                                                        lineage.ColumnList(
                                                            [
                                                                lineage_columns.Select(
                                                                    column,
                                                                    macro_group=macro_group,
                                                                )
                                                                for column in base.columns[
                                                                    base.static_primary_key.list_names()
                                                                ]
                                                            ]
                                                        )
                                                    ),
                                                    macro_group=macro_group,
                                                ),
                                                unit=interval.unit,
                                                macro_group=macro_group,
                                            ),
                                            macro_group=macro_group,
                                        ),
                                        right=interval,
                                        macro_group=macro_group,
                                    ),
                                ],
                                link_operators=[
                                    lineage_operators.And(macro_group=macro_group)
                                ],
                                macro_group=macro_group,
                            )
                        ],
                        values=[
                            lineage_functions.array.Range(
                                start=lineage_functions.datetime.DateBin(
                                    source=lineage_columns.Select(
                                        base.event_time, macro_group=macro_group
                                    ),
                                    interval=interval,
                                    macro_group=macro_group,
                                ),
                                end=lineage_functions.window.Lag(
                                    source=lineage_columns.Select(
                                        base.event_time, macro_group=macro_group
                                    ),
                                    order_by=lineage.OrderBy(
                                        columns=lineage.ColumnList(
                                            [
                                                lineage_columns.Select(
                                                    base.event_time,
                                                    macro_group=macro_group,
                                                )
                                            ]
                                        ),
                                        how=[
                                            lineage_operators.Descending(
                                                macro_group=macro_group
                                            )
                                        ],
                                    ),
                                    partition_by=lineage.PartitionBy(
                                        lineage.ColumnList(
                                            [
                                                lineage_columns.Select(
                                                    column, macro_group=macro_group
                                                )
                                                for column in base.columns[
                                                    base.static_primary_key.list_names()
                                                ]
                                            ]
                                        )
                                    ),
                                    macro_group=macro_group,
                                ),
                                interval=interval,
                                macro_group=macro_group,
                            ),
                        ],
                        else_value=lineage_values.List(
                            values=[
                                lineage_functions.datetime.DateBin(
                                    source=lineage_columns.Select(
                                        base.event_time, macro_group=macro_group
                                    ),
                                    interval=interval,
                                    macro_group=macro_group,
                                )
                            ],
                            macro_group=macro_group,
                        ),
                    ),
                    name="date_array",
                    macro_group=macro_group,
                ),
            ]
        ),
        primary_key=lineage.ColumnList(
            [
                lineage_columns.Select(column, macro_group=macro_group)
                for column in base.primary_key.list_columns()
            ]
        ),
        event_time=lineage_columns.Select(base.event_time, macro_group=macro_group),
        macro_group=macro_group,
    )

    unnested_arrays = lineage_tables.Core(
        name=rf"{source.name}_unnested_arrays",
        ctes=lineage.TableList([base, date_arrays]),
        source=lineage_tables.Select(date_arrays, macro_group=macro_group),
        columns=lineage.ColumnList(
            [
                lineage_columns.Core(
                    name=column.name,
                    source=lineage_columns.Select(column, macro_group=macro_group),
                    macro_group=macro_group,
                )
                for column in date_arrays.primary_key.list_columns()
            ]
            + [
                lineage_columns.Core(
                    source=lineage_functions.array.Unnest(
                        source=lineage_columns.Select(
                            date_arrays.columns.date_array, macro_group=macro_group
                        ),
                        macro_group=macro_group,
                    ),
                    name=rf"trunc_{date_arrays.event_time.name}",
                    macro_group=macro_group,
                )
            ]
        ),
        primary_key=lineage.ColumnList(
            [
                lineage_columns.Select(column, macro_group=macro_group)
                for column in date_arrays.primary_key.list_columns()
            ]
        ),
        event_time=lineage_columns.Select(
            date_arrays.event_time, macro_group=macro_group
        ),
        macro_group=macro_group,
    )

    left = lineage_tables.Select(unnested_arrays, macro_group=macro_group)
    right = lineage_tables.Select(base, macro_group=macro_group)

    row_number_assigned = lineage_tables.Core(
        name=rf"{source.name}_row_number_assigned",
        ctes=lineage.TableList([base, date_arrays, unnested_arrays]),
        source=lineage_combinations.Join(
            join_expression=lineage_expressions.LeftJoin(
                left=left, right=right, macro_group=macro_group
            ),
            condition=lineage.Condition(
                checks=[
                    lineage_expressions.Equal(
                        left=lineage_columns.Select(column, macro_group=macro_group),
                        right=lineage_columns.Select(
                            left.columns[column.name], macro_group=macro_group
                        ),
                        macro_group=macro_group,
                    )
                    for column in right.primary_key.list_columns()
                ],
                link_operators=[
                    lineage_operators.And(macro_group=macro_group)
                    for i in range(len(right.primary_key.list_columns()) - 1)
                ],
            ),
        ),
        columns=lineage.ColumnList(
            [
                lineage_columns.Core(
                    name=column.name,
                    source=lineage_columns.Select(column, macro_group=macro_group),
                    macro_group=macro_group,
                )
                for column in left.columns.list_columns()
            ]
            + [
                lineage_columns.Core(
                    source=lineage_functions.window.RowNumber(
                        partition_by=lineage.PartitionBy(
                            lineage.ColumnList(
                                [
                                    lineage_columns.Select(
                                        column, macro_group=macro_group
                                    )
                                    for column in left.static_primary_key.list_columns()
                                ]
                                + [
                                    lineage_columns.Select(
                                        left.columns[rf"trunc_{left.event_time.name}"],
                                        macro_group=macro_group,
                                    )
                                ]
                            )
                        ),
                        order_by=lineage.OrderBy(
                            columns=lineage.ColumnList(
                                [
                                    lineage_columns.Select(
                                        left.event_time, macro_group=macro_group
                                    )
                                ]
                            ),
                            how=[lineage_operators.Descending(macro_group=macro_group)],
                        ),
                        macro_group=macro_group,
                    ),
                    name="row_num",
                    macro_group=macro_group,
                )
            ]
        ),
        primary_key=lineage.ColumnList(
            [
                lineage_columns.Select(column, macro_group=macro_group)
                for column in left.primary_key.list_columns()
            ]
        ),
        event_time=lineage_columns.Select(left.event_time, macro_group=macro_group),
        macro_group=macro_group,
    )

    max_row_number_source = lineage_tables.Select(
        row_number_assigned, macro_group=macro_group
    )

    max_row_number = lineage_tables.Core(
        name=rf"{source.name}_max_row_number",
        source=max_row_number_source,
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
                lineage_columns.Select(column, macro_group=macro_group)
                for column in max_row_number_source.primary_key.list_columns()
            ]
        ),
        event_time=lineage_columns.Select(
            max_row_number_source.event_time, macro_group=macro_group
        ),
        columns=lineage.ColumnList(
            [
                lineage_columns.Core(
                    name=column.name,
                    source=lineage_columns.Select(column, macro_group=macro_group),
                    macro_group=macro_group,
                )
                for column in max_row_number_source.primary_key.list_columns()
            ]
            + [
                lineage_columns.Core(
                    source=lineage_functions.aggregate.Maximum(
                        source=lineage_columns.Select(
                            max_row_number_source.columns.row_num,
                            macro_group=macro_group,
                        ),
                        macro_group=macro_group,
                    ),
                    name="max_row_num",
                    macro_group=macro_group,
                )
            ]
        ),
        group_by=True,
        macro_group=macro_group,
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
        source=lineage_combinations.CompoundJoin(
            [
                lineage_combinations.Join(
                    join_expression=lineage_expressions.LeftJoin(
                        left=lineage_tables.Select(base, macro_group=macro_group),
                        right=lineage_tables.Select(
                            row_number_assigned, macro_group=macro_group
                        ),
                    ),
                    condition=lineage.Condition(
                        checks=[
                            lineage_expressions.Equal(
                                left=lineage_columns.Select(
                                    base.columns[column.name], macro_group=macro_group
                                ),
                                right=lineage_columns.Select(
                                    row_number_assigned.columns[column.name],
                                    macro_group=macro_group,
                                ),
                                macro_group=macro_group,
                            )
                            for column in base.primary_key.list_columns()
                        ],
                        link_operators=[
                            lineage_operators.And(macro_group=macro_group)
                            for i in range(len(base.primary_key.list_columns()) - 1)
                        ],
                    ),
                    macro_group=macro_group,
                ),
                lineage_combinations.Join(
                    join_expression=lineage_expressions.LeftJoin(
                        left=lineage_tables.Select(
                            row_number_assigned, macro_group=macro_group
                        ),
                        right=lineage_tables.Select(
                            max_row_number, macro_group=macro_group
                        ),
                        macro_group=macro_group,
                    ),
                    condition=lineage.Condition(
                        checks=[
                            lineage_expressions.Equal(
                                left=lineage_columns.Select(
                                    row_number_assigned.columns[column.name],
                                    macro_group=macro_group,
                                ),
                                right=lineage_columns.Select(
                                    max_row_number.columns[column.name],
                                    macro_group=macro_group,
                                ),
                                macro_group=macro_group,
                            )
                            for column in row_number_assigned.primary_key.list_columns()
                        ],
                        link_operators=[
                            lineage_operators.And(macro_group=macro_group)
                            for i in range(
                                len(row_number_assigned.primary_key.list_columns()) - 1
                            )
                        ],
                        macro_group=macro_group,
                    ),
                    macro_group=macro_group,
                ),
            ]
        ),
        columns=lineage.ColumnList(
            [
                lineage_columns.Core(
                    name=column.name,
                    source=lineage_columns.Select(column, macro_group=macro_group),
                    macro_group=macro_group,
                )
                for column in row_number_assigned.static_primary_key.list_columns()
            ]
            + [
                lineage_columns.Core(
                    name=row_number_assigned.event_time.name,
                    source=lineage_columns.Select(
                        row_number_assigned.columns[
                            rf"trunc_{row_number_assigned.event_time.name}"
                        ],
                        var_type="timestamp",
                        macro_group=macro_group,
                    ),
                    macro_group=macro_group,
                ),
            ]
            + [
                lineage_columns.Core(
                    name=column.name,
                    source=lineage_columns.Select(column, macro_group=macro_group),
                    macro_group=macro_group,
                )
                for column in base.columns.list_columns()
                if column.name[:4] != "lag_"
                and column.name not in base.primary_key.list_names()
            ]
        ),
        where_condition=lineage.Condition(
            checks=[
                lineage_expressions.Equal(
                    left=lineage_columns.Select(
                        row_number_assigned.columns.row_num, macro_group=macro_group
                    ),
                    right=lineage_values.Integer(1, macro_group=macro_group),
                    macro_group=macro_group,
                )
            ],
            macro_group=macro_group,
        ),
        event_time=lineage_columns.Select(
            row_number_assigned.columns[
                rf"trunc_{row_number_assigned.event_time.name}"
            ],
            alias=row_number_assigned.event_time.name,
            macro_group=macro_group,
        ),
        primary_key=lineage.ColumnList(
            [
                lineage_columns.Select(column, macro_group=macro_group)
                for column in base.static_primary_key.list_columns()
            ]
            + [
                lineage_columns.Select(
                    row_number_assigned.columns[
                        rf"trunc_{row_number_assigned.event_time.name}"
                    ],
                    alias=row_number_assigned.event_time.name,
                    macro_group=macro_group,
                )
            ]
        ),
        macro_group=macro_group,
    )

    table = lineage_tables.Core(
        name=name,
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
        source=lineage_tables.Select(transformed, macro_group=macro_group),
        columns=lineage.ColumnList(
            [
                lineage_columns.Core(
                    name=column.name,
                    source=lineage_columns.Select(column),
                    macro_group=macro_group,
                )
                for column in transformed.columns.list_columns()
            ]
        ),
        event_time=lineage_columns.Select(
            transformed.event_time, macro_group=macro_group
        ),
        primary_key=lineage.ColumnList(
            [
                lineage_columns.Select(column, macro_group=macro_group)
                for column in transformed.primary_key.list_columns()
            ]
        ),
    )

    return table


def date_vector_table(name: str, start_time, n_records: int, interval):
    macro_group = (
        rf"DateVector:{name} - {start_time.sql} - {str(n_records)} - {interval.sql}"
    )

    return lineage_tables.Core(
        name=name,
        source=lineage.RecordList(
            name="date_vector",
            records=[
                lineage.Record(
                    {
                        lineage_columns.Record(
                            name="date",
                            var_type="timestamp",
                            data_type=lineage_values.Datatype(
                                "TIMESTAMP", macro_group=macro_group
                            ),
                            macro_group=macro_group,
                        ): lineage_functions.datetime.DateAdd(
                            start_time,
                            lineage_functions.math.Multiply(
                                lineage_values.Integer(i, macro_group=macro_group),
                                interval,
                                macro_group=macro_group,
                            ),
                            macro_group=macro_group,
                        )
                    },
                    macro_group=macro_group,
                )
                for i in range(n_records)
            ],
            macro_group=macro_group,
        ),
        macro_group=macro_group,
    )


def forward_fill(source):
    macro_group = rf"ForwardFill - {id(source)}"

    if type(source) is lineage_tables.Select:
        source = source
        ctes = lineage.TableList([])
    else:
        source = lineage_tables.Select(source, macro_group=macro_group)
        ctes = lineage.TableList([source])

    source_grouped = lineage_tables.Core(
        name=rf"{source.name}_grouped",
        source=source,
        ctes=ctes,
        columns=lineage.ColumnList(
            [
                lineage_columns.Core(
                    name=column.name,
                    source=lineage_columns.Select(column, macro_group=macro_group),
                    macro_group=macro_group,
                )
                for column in source.columns.list_columns()
            ]
            + [
                lineage_columns.Core(
                    lineage_functions.aggregate.Count(
                        source=lineage_columns.Select(column, macro_group=macro_group),
                        partition_by=lineage.PartitionBy(
                            lineage.ColumnList(
                                [
                                    lineage_columns.Select(
                                        column, macro_group=macro_group
                                    )
                                    for column in source.static_primary_key.list_columns()
                                ]
                            )
                        ),
                        order_by=lineage.OrderBy(
                            columns=lineage.ColumnList(
                                [
                                    lineage_columns.Select(
                                        source.event_time, macro_group=macro_group
                                    )
                                ]
                            ),
                            how=[lineage_operators.Descending(macro_group=macro_group)],
                        ),
                        macro_group=macro_group,
                    ),
                    macro_group=macro_group,
                    name=rf"{column.name}_count",
                )
                for column in source.columns.list_columns()
                if column.name not in source.primary_key.list_names()
            ]
        ),
        primary_key=lineage.ColumnList(
            [
                lineage_columns.Select(column, macro_group=macro_group)
                for column in source.primary_key.list_columns()
            ]
        ),
        event_time=lineage_columns.Select(source.event_time, macro_group=macro_group),
    )

    source_forward_filled = lineage_tables.Core(
        name=rf"{source.name}_forward_filled",
        ctes=lineage.TableList(
            [table for table in source_grouped.ctes.list_tables()]
            + [
                source_grouped,
            ]
        ),
        columns=lineage.ColumnList(
            [
                lineage_columns.Core(
                    name=column.name,
                    source=lineage_columns.Select(column, macro_group=macro_group),
                    macro_group=macro_group,
                )
                for column in source_grouped.primary_key.list_columns()
            ]
            + [
                lineage_columns.Core(
                    source=lineage_functions.aggregate.Maximum(
                        lineage_columns.Select(
                            source_grouped.columns[column.name], macro_group=macro_group
                        ),
                        partition_by=lineage.PartitionBy(
                            lineage.ColumnList(
                                [
                                    lineage_columns.Select(spk, macro_group=macro_group)
                                    for spk in source_grouped.static_primary_key.list_columns()
                                ]
                                + [
                                    lineage_columns.Select(
                                        source_grouped.columns[rf"{column.name}_count"],
                                        macro_group=macro_group,
                                    )
                                ]
                            ),
                        ),
                        macro_group=macro_group,
                    ),
                    name=column.name,
                    macro_group=macro_group,
                )
                for column in source_grouped.columns.list_columns()
                if column.name
                in [
                    column
                    for column in source.columns.list_names()
                    if column not in source.primary_key.list_names()
                ]
            ]
        ),
        source=lineage_tables.Select(source_grouped, macro_group=macro_group),
        event_time=lineage_columns.Select(
            source_grouped.event_time, macro_group=macro_group
        ),
        primary_key=lineage.ColumnList(
            [
                lineage_columns.Select(column, macro_group=macro_group)
                for column in source_grouped.primary_key.list_columns()
            ]
        ),
        macro_group=macro_group,
    )

    return source_forward_filled


def staging_table_transform(source: lineage_tables.Core, settings=None):
    macro_group = rf"StagingTableTransform - {id(source)}"

    expected_column_metadata = source.source.expected_column_metadata

    event_time = [column for column in expected_column_metadata if column.is_event_time]

    if len(event_time) > 1:
        print("\n".join([str(column.__dict__) for column in expected_column_metadata]))
        raise ValueError("More than 1 event_time defined")
    elif len(event_time) == 1:
        event_time = staging_column_transform(
            source_column=source.columns.list_columns()[0],
            column_metadata=event_time[0],
        )
    else:
        event_time = None

    checks = []

    if (
        event_time
        and settings.configuration["min_event_time"]
        and settings.configuration["max_event_time"]
    ):
        checks.append(
            lineage_expressions.Between(
                left=event_time,
                right=lineage_expressions.And(
                    lineage_values.Timestamp(
                        settings.configuration["min_event_time"].strftime(
                            "%Y-%m-%d %H:%M:%S"
                        )
                    ),
                    lineage_values.Timestamp(
                        settings.configuration["max_event_time"].strftime(
                            "%Y-%m-%d %H:%M:%S"
                        )
                    ),
                ),
            )
        )

    elif event_time and settings.configuration["min_event_time"]:
        checks.append(
            lineage_expressions.GreaterThanOrEqual(
                left=event_time,
                right=lineage_values.Timestamp(
                    settings.configuration["min_event_time"].strftime(
                        "%Y-%m-%d %H:%M:%S"
                    )
                ),
            )
        )
    elif event_time and settings.configuration["max_event_time"]:
        checks.append(
            lineage_expressions.LessThanOrEqual(
                left=event_time,
                right=lineage_values.Timestamp(
                    settings.configuration["max_event_time"].strftime(
                        "%Y-%m-%d %H:%M:%S"
                    )
                ),
            )
        )
    else:
        pass

    if checks:
        where_condition = lineage.Condition(
            checks=checks,
            link_operators=[lineage_operators.And() for i in range(len(checks) - 1)],
        )
    else:
        where_condition = None

    return lineage_tables.Core(
        name=source.name,
        source=lineage_tables.Select(source, macro_group=macro_group),
        columns=lineage.ColumnList(
            [
                staging_column_transform(
                    source_column=source.columns.list_columns()[0],
                    column_metadata=column_metadata,
                )
                for column_metadata in expected_column_metadata
            ]
        ),
        distinct=source.distinct,
        primary_key=lineage.ColumnList(
            [
                staging_column_transform(
                    source_column=source.columns.list_columns()[0],
                    column_metadata=column_metadata,
                )
                for column_metadata in expected_column_metadata
                if column_metadata.is_primary_key
            ]
        ),
        event_time=event_time,
        where_condition=where_condition,
    )
