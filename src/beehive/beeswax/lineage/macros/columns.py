from .core import Macro
from ...lineage import core as lineage
from ...lineage import values as lineage_values
from ...lineage import functions as lineage_functions
from ...lineage import columns as lineage_columns
from ...lineage import aggregates as lineage_aggregates
from ...lineage import expressions as lineage_expressions
from ...lineage import tables as lineage_tables
from ...lineage.macros import functions as macro_functions


def source_transform(source_column: lineage_columns.Source):
    column = lineage_functions.TryCast(
        source=source_column,
        data_type=source_column.data_type,
    )

    if (source_column.regex != "") & (source_column.var_type == "timestamp"):
        if (source_column.regex == "datetime") & (
            source_column.data_type.value == "TIMESTAMP"
        ):
            column = lineage_functions.TryCast(
                source=source_column,
                data_type=lineage_values.Datatype("TIMESTAMP"),
            )

        elif (
            source_column.data_type.value == "TIMESTAMP"
            and source_column.regex == "epoch_ms"
        ):
            column = lineage_functions.EpochMSToTimestamp(
                lineage_functions.TryCast(
                    source=source_column,
                    data_type=lineage_values.Datatype("INTEGER"),
                ),
            )
        elif (
            source_column.data_type.value == "TIMESTAMP"
            and source_column.regex == "epoch_s"
        ):
            column = lineage_functions.EpochToTimestamp(
                lineage_functions.TryCast(
                    source=source_column,
                    data_type=lineage_values.Datatype("INTEGER"),
                ),
            )
        else:
            column = lineage_functions.StringToTimestamp(
                lineage_functions.TryCast(
                    source=source_column,
                    data_type=lineage_values.Datatype("VARCHAR"),
                ),
                timestamp_format=lineage_values.Varchar(source_column.regex),
            )

    elif (source_column.regex != "") & (source_column.var_type == "timedelta"):
        if (source_column.regex == "datetime") & (
            source_column.data_type.value == "INTERVAL"
        ):
            column = lineage_functions.ToInterval(
                source=lineage_functions.TryCast(
                    source=source_column,
                    data_type=lineage_values.Datatype("TIMESTAMP"),
                ),
                unit=source_column.source_unit,
            )

        elif source_column.data_type.value == "INTERVAL":
            column = lineage_functions.DateDiff(
                start=lineage_functions.StringToTimestamp(
                    lineage_functions.TryCast(
                        source=source_column,
                        data_type=lineage_values.Datatype("VARCHAR"),
                    ),
                    timestamp_format=lineage_values.Varchar(source_column.regex),
                ),
                end=lineage_functions.StringToTimestamp(
                    lineage_functions.TryCast(
                        source=source_column,
                        data_type=lineage_values.Datatype("VARCHAR"),
                    ),
                    timestamp_format=lineage_values.Varchar(source_column.regex),
                ),
                unit=lineage.units.core.Unit("ms^1"),
            )
    elif (source_column.var_type == "timedelta") & (
        len(source_column.unit.sub_units) > 0
    ):
        column = lineage_functions.ToInterval(
            source=lineage_functions.TryCast(
                source_column, lineage_values.Datatype("INTEGER")
            ),
            unit=source_column.source_unit,
        )

    elif (source_column.regex != "") and (source_column.data_type.value == "VARCHAR"):
        column = lineage_functions.RegExpExtract(
            source=lineage_functions.TryCast(
                source=source_column,
                data_type=lineage_values.Datatype("VARCHAR"),
            ),
            regex=lineage_values.Varchar(source_column.regex),
        )

    elif source_column.regex != "":
        column = lineage_functions.TryCast(
            source=lineage_functions.RegExpExtract(
                source=lineage_functions.TryCast(
                    source=source_column,
                    data_type=lineage_values.Datatype("VARCHAR"),
                ),
                regex=source_column.regex,
            ),
            data_type=lineage_values.Datatype(source_column.data_type.value),
        )

    elif source_column.on_null == "FAIL":
        column = lineage.CaseWhen(
            conditions=[
                lineage.Condition(
                    checks=[
                        lineage_expressions.Is(left=column, right=lineage_values.Null())
                    ]
                )
            ],
            values=[
                lineage_functions.Error(
                    lineage_values.Varchar(
                        rf"NULL encountered in column: {source_column.name}"
                    )
                )
            ],
            else_value=column,
        )

    column = macro_functions.unit_conversion.convert_to_unit(
        column, unit=source_column.target_unit
    )

    if source_column.column_metadata["precision"].iloc[0]:
        if source_column.column_metadata["precision"].iloc[0][-2:] == "sf":
            column = macro_functions.numeric.significant_figures(
                column,
                lineage_values.Integer(
                    int(source_column.column_metadata["precision"].iloc[0][:-2])
                ),
            )
        elif source_column.column_metadata["precision"].iloc[0][-2:] == "dp":
            column = lineage_functions.Round(
                column,
                lineage_values.Integer(
                    int(source_column.column_metadata["precision"].iloc[0][:-2])
                ),
            )

    output = lineage_columns.Expand(
        source=column,
        name=source_column.name,
    )

    setattr(output, "on_null", source_column.on_null)
    setattr(output, "filter_values", source_column.filter_values)
    setattr(output, "on_filter", source_column.on_filter)
    setattr(output, "is_primary_key", source_column.is_primary_key)
    setattr(output, "is_event_time", source_column.is_event_time)
    setattr(output, "var_type", source_column.var_type)

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


class SourceTransform(Macro):
    def __init__(self, source_column: lineage_columns.Source):
        super().__init__(
            name=rf"SourceTransform", macro=source_transform(source_column)
        )
        self.args = [source_column]
