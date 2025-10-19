"""
DataFrame intro
"""

import units

from ..lineage import core as lineage
import typing


class DataFrameColumn(lineage._Column):
    def __init__(self, source: lineage._Column, macro_group: str = ""):
        super().__init__(
            source=source,
            name=source.name,
            data_type=source.data_type,
            var_type=source.var_type,
            on_null=source.on_null,
            is_event_time=source.is_event_time,
            is_primary_key=source.is_primary_key,
            macro_group=macro_group,
        )


class LambdaFunction(lineage._Function):
    def __init__(
        self,
        name: str,
        function: callable,
        args: typing.Dict[str, any],
        data_type,
        var_type=None,
        unit=units.core.Unit(),
        macro_group: str = "",
    ):
        super().__init__(
            name=name,
            args=list(args.values()),
            data_type=data_type,
            var_type=var_type,
            unit=unit,
            macro_group=macro_group,
        )

        self.function = function

        self.function_args = {
            arg: args[arg].name
            for arg in args.keys()
            if type(args[arg]) is DataFrameColumn
        } | {
            arg: args[arg].value
            for arg in args.keys()
            if type(args[arg]).__bases__[0] is lineage._Value
        }


class LambdaOutput(lineage._Column):
    def __init__(self, name: str, source: LambdaFunction, macro_group: str = ""):
        super().__init__(
            name=name,
            source=source,
            data_type=source.data_type,
            var_type=source.var_type,
            is_event_time=False,
            is_primary_key=False,
            on_null="PASS",
            macro_group=macro_group,
        )


class DataFrame(lineage._Table):
    def __init__(
        self,
        name: str,
        source: lineage._Table,
        columns: lineage.ColumnList,
        macro_group: str = "",
    ):
        if any(
            [
                type(column) not in [DataFrameColumn, LambdaOutput]
                for column in columns.list_columns_()
            ]
        ):
            raise ValueError(
                "All columns must be either DataFrameColumn or LambdaColumn"
            )

        if source.event_time:
            event_time = columns[source.event_time.name]
        else:
            event_time = None

        super().__init__(
            name=name,
            source=source,
            columns=columns,
            primary_key=lineage.ColumnList(
                [
                    columns[column_name]
                    for column_name in source.primary_key.list_names_()
                ]
            ),
            event_time=event_time,
            macro_group=macro_group,
        )
