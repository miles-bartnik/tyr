import units.core

from .. import core as lineage
from .. import values as lineage_values
from .. import columns as lineage_columns
from .. import functions as lineage_functions
from .. import expressions as lineage_expressions
from . import functions as macro_functions


def select_all(table: lineage._Table):
    macro_group = rf"SelectAll - {id(table)}"

    select_columns = lineage.ColumnList(
        [
            lineage_columns.Core(
                name=column.name,
                source=lineage_columns.Select(column, macro_group=macro_group),
                macro_group=macro_group,
            )
            for column in table.columns.list_all()
        ]
    )

    return select_columns

def select_primary_key(table: lineage._Table):
    macro_group = rf"SelectPrimaryKey - {id(table)}"

    select_columns = lineage.ColumnList(
        [
            lineage_columns.Core(
                name=column.name,
                source=lineage_columns.Select(column, macro_group=macro_group),
                macro_group=macro_group,
            )
            for column in table.primary_key.list_all()
        ]
    )

    return select_columns

def select_static_primary_key(table: lineage._Table):
    macro_group = rf"SelectStaticPrimaryKey - {id(table)}"

    select_columns = lineage.ColumnList(
        [
            lineage_columns.Core(
                name=column.name,
                source=lineage_columns.Select(column, macro_group=macro_group),
                macro_group=macro_group,
            )
            for column in table.static_primary_key.list_all()
        ]
    )

    return select_columns


def staging_column_transform(source_column: lineage_columns.WildCard, column_metadata):
    if "lineage.schema.source.ColumnMetadata" not in str(type(column_metadata)):
        raise ValueError("column_metadata must be ColumnMetadata object")

    macro_group = rf"StagingColumnTransform - {id(source_column)}"

    column = tyr.lineage.functions.data_type.TryCast(
        source=lineage_functions.utility.SourceWildToStagingColumn(
            source_column, column_metadata, macro_group=macro_group
        ),
        data_type=column_metadata.data_type,
        macro_group=macro_group,
    )

    source_unit = units.core.Unit(column_metadata.source_unit, macro_group=macro_group)
    target_unit = units.core.Unit(column_metadata.target_unit, macro_group=macro_group)

    print(column_metadata.column_name)
    print(column_metadata.column_alias)
    print(column_metadata.regex)
    print(column_metadata.var_type)
    print(column_metadata.data_type.__dict__)
    print("\n")

    if (column_metadata.regex != "") & (column_metadata.var_type == "timestamp"):
        if (column_metadata.regex == "datetime") & (
            column_metadata.data_type.value == "TIMESTAMP"
        ):
            column = tyr.lineage.functions.data_type.TryCast(
                source=lineage_functions.utility.SourceWildToStagingColumn(
                    source_column, column_metadata, macro_group=macro_group
                ),
                data_type=lineage_values.Datatype("TIMESTAMP", macro_group=macro_group),
                macro_group=macro_group,
            )

        elif (
            column_metadata.data_type.value == "TIMESTAMP"
            and column_metadata.regex == "epoch_ms"
        ):
            column = lineage_functions.datetime.EpochMSToTimestamp(
                tyr.lineage.functions.data_type.TryCast(
                    source=lineage_functions.utility.SourceWildToStagingColumn(
                        source_column, column_metadata, macro_group=macro_group
                    ),
                    data_type=lineage_values.Datatype(
                        "INTEGER", macro_group=macro_group
                    ),
                    macro_group=macro_group,
                ),
                macro_group=macro_group,
            )
        elif (
            column_metadata.data_type.value == "TIMESTAMP"
            and column_metadata.regex == "epoch_s"
        ):
            column = lineage_functions.datetime.EpochToTimestamp(
                tyr.lineage.functions.data_type.TryCast(
                    source=lineage_functions.utility.SourceWildToStagingColumn(
                        source_column, column_metadata, macro_group=macro_group
                    ),
                    data_type=lineage_values.Datatype(
                        "INTEGER", macro_group=macro_group
                    ),
                    macro_group=macro_group,
                ),
                macro_group=macro_group,
            )
        else:
            column = lineage_functions.datetime.StringToTimestamp(
                tyr.lineage.functions.data_type.TryCast(
                    source=lineage_functions.utility.SourceWildToStagingColumn(
                        source_column, column_metadata, macro_group=macro_group
                    ),
                    data_type=lineage_values.Datatype(
                        "VARCHAR", macro_group=macro_group
                    ),
                    macro_group=macro_group,
                ),
                timestamp_format=lineage_values.Varchar(
                    column_metadata.regex, macro_group=macro_group
                ),
                macro_group=macro_group,
            )

    elif (column_metadata.regex != "") & (column_metadata.var_type == "timedelta"):
        if (column_metadata.regex == "datetime") & (
            column_metadata.data_type.value == "INTERVAL"
        ):
            column = tyr.lineage.functions.data_type.ToInterval(
                source=tyr.lineage.functions.data_type.TryCast(
                    source=lineage_functions.utility.SourceWildToStagingColumn(
                        source_column, column_metadata, macro_group=macro_group
                    ),
                    data_type=lineage_values.Datatype(
                        "TIMESTAMP", macro_group=macro_group
                    ),
                    macro_group=macro_group,
                ),
                macro_group=macro_group,
                unit=source_unit,
            )

        elif column_metadata.data_type.value == "INTERVAL":
            column = lineage_functions.datetime.DateDiff(
                start=lineage_functions.datetime.StringToTimestamp(
                    tyr.lineage.functions.data_type.TryCast(
                        source=lineage_functions.utility.SourceWildToStagingColumn(
                            source_column, column_metadata, macro_group=macro_group
                        ),
                        data_type=lineage_values.Datatype(
                            "VARCHAR", macro_group=macro_group
                        ),
                        macro_group=macro_group,
                    ),
                    timestamp_format=lineage_values.Varchar(
                        column_metadata.regex, macro_group=macro_group
                    ),
                    macro_group=macro_group,
                ),
                end=lineage_functions.datetime.StringToTimestamp(
                    tyr.lineage.functions.data_type.TryCast(
                        source=lineage_functions.utility.SourceWildToStagingColumn(
                            source_column, column_metadata, macro_group=macro_group
                        ),
                        data_type=lineage_values.Datatype("VARCHAR"),
                        macro_group=macro_group,
                    ),
                    timestamp_format=lineage_values.Varchar(column_metadata.regex),
                    macro_group=macro_group,
                ),
                unit=lineage.units.core.Unit("ms^1", macro_group=macro_group),
                macro_group=macro_group,
            )
    elif (column_metadata.var_type == "timedelta") & (len(source_unit.sub_units) > 0):
        column = tyr.lineage.functions.data_type.ToInterval(
            source=tyr.lineage.functions.data_type.TryCast(
                lineage_functions.utility.SourceWildToStagingColumn(
                    source_column, column_metadata, macro_group=macro_group
                ),
                lineage_values.Datatype("INTEGER", macro_group=macro_group),
                macro_group=macro_group,
            ),
            unit=source_unit,
            macro_group=macro_group,
        )

    elif (column_metadata.regex != "") and (
        column_metadata.data_type.value == "VARCHAR"
    ):
        column = lineage_functions.string.RegExpExtract(
            source=tyr.lineage.functions.data_type.TryCast(
                source=lineage_functions.utility.SourceWildToStagingColumn(
                    source_column, column_metadata, macro_group=macro_group
                ),
                data_type=lineage_values.Datatype("VARCHAR", macro_group=macro_group),
                macro_group=macro_group,
            ),
            regex=lineage_values.Varchar(
                column_metadata.regex, macro_group=macro_group
            ),
            macro_group=macro_group,
        )

    elif column_metadata.regex != "":
        column = tyr.lineage.functions.data_type.TryCast(
            source=lineage_functions.string.RegExpExtract(
                source=tyr.lineage.functions.data_type.TryCast(
                    source=lineage_functions.utility.SourceWildToStagingColumn(
                        source_column, column_metadata, macro_group=macro_group
                    ),
                    data_type=lineage_values.Datatype(
                        "VARCHAR", macro_group=macro_group
                    ),
                    macro_group=macro_group,
                ),
                regex=lineage_values.Varchar(
                    column_metadata.regex, macro_group=macro_group
                ),
                macro_group=macro_group,
            ),
            data_type=lineage_values.Datatype(
                column_metadata.data_type, macro_group=macro_group
            ),
            macro_group=macro_group,
        )

    elif column_metadata.on_null == "FAIL":
        column = lineage.CaseWhen(
            conditions=[
                lineage.Condition(
                    checks=[
                        lineage_expressions.Is(
                            left=column,
                            right=lineage_values.Null(macro_group=macro_group),
                            macro_group=macro_group,
                        )
                    ],
                    macro_group=macro_group,
                )
            ],
            values=[
                lineage_functions.utility.Error(
                    lineage_values.Varchar(
                        rf"NULL encountered in column: {column_metadata['column_name']}",
                        macro_group=macro_group,
                    ),
                    macro_group=macro_group,
                )
            ],
            else_value=column,
            macro_group=macro_group,
        )

    column = macro_functions.unit_conversion.convert_to_unit(column, unit=target_unit)

    if column_metadata.precision:
        if column_metadata.precision[-2:] == "sf":
            column = tyr.lineage.macros.functions.numeric.significant_figures(
                column,
                lineage_values.Integer(int(column_metadata.precision[:-2])),
            )
        elif column_metadata.precision[-2:] == "dp":
            column = lineage_functions.math.Round(
                column,
                lineage_values.Integer(int(column_metadata.precision[:-2])),
            )

    if column_metadata.column_alias:
        column_name = column_metadata.column_alias
    else:
        column_name = column_metadata.column_name

    output = lineage_columns.Core(
        source=column, name=column_name, macro_group=macro_group
    )

    setattr(output, "on_null", column_metadata.on_null)
    setattr(output, "filter_values", column_metadata.filter_values)
    setattr(output, "on_filter", column_metadata.on_filter)
    setattr(output, "is_primary_key", column_metadata.is_primary_key)
    setattr(output, "is_event_time", column_metadata.is_event_time)
    setattr(output, "var_type", column_metadata.var_type)

    return output


def blank_clone(source: lineage._Column):
    return lineage_columns.Blank(
        name=source.name,
        var_type=source.var_type,
        data_type=source.data_type,
        on_null=source.on_null,
        is_primary_key=source.is_primary_key,
        is_event_time=source.is_event_time,
        unit=source.unit,
    )
