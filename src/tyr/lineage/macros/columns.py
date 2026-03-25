import warnings

from ..units.core import Unit

from .. import core as lineage
from .. import values as lineage_values
from .. import columns as lineage_columns
from .. import functions as lineage_functions
from .. import expressions as lineage_expressions
from . import functions as macro_functions
from . import values as macro_values


def select_primary_key(
    table: lineage._Table, filter_unit: Unit = None, filter_regex: str = None
):
    select_columns = lineage.ColumnList(
        [
            lineage_columns.Core(
                name=column.name,
                source=lineage_columns.Select(column),
            )
            for column in table.primary_key.list_columns(
                filter_regex=filter_regex, filter_unit=filter_unit
            )
        ]
    )

    return select_columns


def select_static_primary_key(
    table: lineage._Table, filter_unit: Unit = None, filter_regex: str = None
):
    select_columns = lineage.ColumnList(
        [
            lineage_columns.Core(
                name=column.name,
                source=lineage_columns.Select(column),
            )
            for column in table.static_primary_key.list_columns(
                filter_regex=filter_regex, filter_unit=filter_unit
            )
        ]
    )

    return select_columns


def select_all(
    table: lineage._Table,
    filter_regex: str = None,
    filter_unit: Unit = None,
    primary_key: bool = True,
    static_primary_key: bool = False,
    apply_filters_to_primary_key: bool = True,
):
    if primary_key and static_primary_key:
        warnings.warn(
            "Both primary_key and static_primary_key selected. Defaulting to primary_key"
        )

    if primary_key and table.primary_key:
        if apply_filters_to_primary_key:
            pk_columns = select_primary_key(
                table, filter_unit=filter_unit, filter_regex=filter_regex
            )
        else:
            pk_columns = select_primary_key(table)
    elif static_primary_key and table.static_primary_key:
        if apply_filters_to_primary_key:
            pk_columns = select_static_primary_key(
                table, filter_unit=filter_unit, filter_regex=filter_regex
            )
        else:
            pk_columns = select_primary_key(table)
    else:
        pk_columns = lineage.ColumnList([])

    select_columns = lineage.ColumnList(
        [
            lineage_columns.Core(
                name=column.name,
                source=lineage_columns.Select(column),
            )
            for column in table.columns.list_columns(
                filter_regex=filter_regex, filter_unit=filter_unit
            )
            if column.name not in table.primary_key.list_names()
        ]
    )

    return pk_columns + select_columns


def staging_column_transform(source_column: lineage_columns.WildCard, column_metadata):
    if "lineage.schema.source.ColumnMetadata" not in str(type(column_metadata)):
        raise ValueError("column_metadata must be ColumnMetadata object")

    source_unit = column_metadata.source_unit
    target_unit = column_metadata.target_unit

    # Initial Read

    source_column = lineage_functions.utility.SourceWildToStagingColumn(
        source_column, column_metadata
    )

    # Casting

    if column_metadata.on_null == "FAIL":
        CastFunction = lineage_functions.data_type.Cast

    else:
        CastFunction = lineage_functions.data_type.TryCast

    if column_metadata.data_type == lineage_values.Datatype("INTERVAL"):
        if column_metadata.regex:
            interval_regex = column_metadata.regex

            for row in lineage.DATE_SPECIFIERS:
                if row["interval_translate"]:
                    if (row["interval_translate"]["regex"] != "") and (
                        row["specifier"] in interval_regex
                    ):
                        interval_regex = interval_regex.replace(
                            row["specifier"], row["interval_translate"]["regex"]
                        )

            interval_regex = lineage_values.Varchar(interval_regex)

            source_column = CastFunction(
                lineage.CaseWhen(
                    conditions=[
                        lineage.Condition(
                            checks=[
                                lineage_functions.string.RegExpMatch(
                                    CastFunction(
                                        source_column,
                                        lineage_values.Datatype("VARCHAR"),
                                    ),
                                    interval_regex,
                                )
                            ]
                        )
                    ],
                    values=[
                        lineage_functions.string.RegExpExtract(
                            CastFunction(
                                source_column, lineage_values.Datatype("VARCHAR")
                            ),
                            interval_regex,
                        )
                    ],
                    else_value=lineage_values.Null(data_type=column_metadata.data_type),
                ),
                data_type=lineage_values.Datatype("INTERVAL"),
            )
        else:
            source_column = CastFunction(
                source=source_column, data_type=lineage_values.Datatype("INTERVAL")
            )

    elif column_metadata.regex and column_metadata.var_type not in [
        "timestamp",
        "date",
    ]:
        source_column = lineage.CaseWhen(
            conditions=[
                lineage.Condition(
                    checks=[
                        lineage_functions.string.RegExpMatch(
                            CastFunction(
                                source_column, lineage_values.Datatype("VARCHAR")
                            ),
                            lineage_values.Varchar(column_metadata.regex),
                        )
                    ]
                )
            ],
            values=[
                lineage_functions.string.RegExpExtract(
                    CastFunction(source_column, lineage_values.Datatype("VARCHAR")),
                    lineage_values.Varchar(column_metadata.regex),
                )
            ],
            else_value=lineage_values.Null(data_type=column_metadata.data_type),
        )

    elif column_metadata.regex != "" and column_metadata.var_type == "timestamp":
        if (column_metadata.regex == "datetime") & (
            column_metadata.data_type.value == "TIMESTAMP"
        ):
            source_column = CastFunction(
                source=source_column,
                data_type=lineage_values.Datatype("TIMESTAMP"),
            )

        elif (
            column_metadata.data_type.value == "TIMESTAMP"
            and column_metadata.regex == "epoch_ms"
        ):
            source_column = lineage_functions.datetime.EpochMSToTimestamp(
                CastFunction(
                    source=source_column,
                    data_type=lineage_values.Datatype("INTEGER"),
                ),
            )
        elif (
            column_metadata.data_type.value == "TIMESTAMP"
            and column_metadata.regex == "epoch_s"
        ):
            source_column = lineage_functions.datetime.EpochToTimestamp(
                CastFunction(
                    source=source_column,
                    data_type=lineage_values.Datatype("INTEGER"),
                ),
            )
        elif column_metadata.regex[-5:] == "%S.%g":
            source_column = lineage_functions.datetime.StringToTimestamp(
                macro_functions.datetime.zero_pad_timestamp(
                    CastFunction(
                        source=source_column,
                        data_type=lineage_values.Datatype("VARCHAR"),
                    )
                ),
                timestamp_format=lineage_values.Varchar(column_metadata.regex),
            )
        else:
            source_column = lineage_functions.datetime.StringToTimestamp(
                CastFunction(
                    source=source_column,
                    data_type=lineage_values.Datatype("VARCHAR"),
                ),
                timestamp_format=lineage_values.Varchar(column_metadata.regex),
            )

    elif (column_metadata.regex != "") and (column_metadata.var_type == "date"):
        date_regex = column_metadata.regex

        for row in lineage.DATE_SPECIFIERS:
            if row["interval_translate"]:
                if (row["interval_translate"]["regex"] != "") and (
                    row["specifier"] in date_regex
                ):
                    date_regex = date_regex.replace(
                        row["specifier"], row["interval_translate"]["regex"]
                    )

        date_regex = lineage_values.Varchar(date_regex)

        source_column = CastFunction(
            lineage_functions.datetime.StringToTimestamp(
                lineage_functions.string.RegExpExtract(
                    source=CastFunction(
                        source=source_column,
                        data_type=lineage_values.Datatype("VARCHAR"),
                    ),
                    regex=date_regex,
                ),
                lineage_values.Varchar(column_metadata.regex),
            ),
            column_metadata.data_type,
        )

    elif (column_metadata.regex != "") and (
        column_metadata.data_type.value == "VARCHAR"
    ):
        source_column = lineage_functions.string.RegExpExtract(
            source=CastFunction(
                source=source_column,
                data_type=lineage_values.Datatype("VARCHAR"),
            ),
            regex=lineage_values.Varchar(column_metadata.regex),
        )

    elif column_metadata.regex != "":
        source_column = CastFunction(
            source=lineage_functions.string.RegExpExtract(
                source=CastFunction(
                    source=source_column,
                    data_type=lineage_values.Datatype("VARCHAR"),
                ),
                regex=lineage_values.Varchar(column_metadata.regex),
            ),
            data_type=column_metadata.data_type,
        )
    else:
        source_column = CastFunction(
            source=source_column,
            data_type=column_metadata.data_type,
        )
    # Filtering

    if column_metadata.filter_values:
        if column_metadata.on_filter == "PASS":
            value = source_column
        elif column_metadata.on_filter == "FAIL":
            value = lineage_functions.utility.Error(
                lineage_values.Varchar(
                    rf"""
            filter_value in [{', '.join(["'" + filter_value + "'" for filter_value in column_metadata.filter_values])}] encountered. Triggered on_filter=FAIL"""
                )
            )
        elif column_metadata.on_filter == "WARN":
            value = source_column
        elif (
            column_metadata.on_filter == "DEFAULT"
            and column_metadata.default_value
            != lineage_values.Null(column_metadata.data_type)
        ):
            value = lineage_functions.utility.Coalesce(
                [source_column, column_metadata.default_value]
            )
        else:
            value = source_column

        source_column = lineage.CaseWhen(
            conditions=[
                lineage.Condition(
                    checks=[
                        lineage_expressions.In(
                            source_column,
                            lineage_values.List(
                                [
                                    lineage_values.Varchar(value)
                                    for value in column_metadata.filter_values
                                ]
                            ),
                        )
                    ]
                )
            ],
            values=[value],
            else_value=source_column,
        )

    if column_metadata.scale_factor:
        if column_metadata.scale_factor != 1:
            source_column = lineage_functions.math.Multiply(
                source_column,
                lineage_values.FloatingPoint(column_metadata.scale_factor),
            )

    if (source_unit != target_unit) and target_unit.name != "":
        source_column = macro_functions.unit_conversion.convert_to_unit(
            source=source_column, target_unit=target_unit
        )

    # if column_metadata.

    if column_metadata.precision:
        if column_metadata.precision[-2:] == "sf":
            source_column = macro_functions.numeric.significant_figures(
                source_column,
                lineage_values.Integer(int(column_metadata.precision[:-2])),
            )
        elif column_metadata.precision[-2:] == "dp":
            source_column = lineage_functions.math.Round(
                source_column,
                lineage_values.Integer(int(column_metadata.precision[:-2])),
            )

    if column_metadata.column_alias:
        column_name = column_metadata.column_alias
    else:
        column_name = column_metadata.column_name

    output = lineage_columns.Core(source=source_column, name=column_name)

    setattr(output, "on_null", column_metadata.on_null)
    setattr(output, "filter_values", column_metadata.filter_values)
    setattr(output, "on_filter", column_metadata.on_filter)
    setattr(output, "is_primary_key", column_metadata.is_primary_key)
    setattr(output, "is_event_time", column_metadata.is_event_time)
    setattr(output, "var_type", column_metadata.var_type)

    return output
