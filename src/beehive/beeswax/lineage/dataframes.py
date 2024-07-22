import units.core

from ..lineage import core as lineage
from ..lineage import columns as lineage_columns
from ..lineage import values as lineage_values
from ..lineage import joins as lineage_combinations
from ..lineage import expressions as lineage_expressions
from ..lineage import operators as lineage_operators
import networkx as nx
import pandas as pd
import typing


class DataFrameColumn(lineage._Column):
    def __init__(self, source: lineage._Column):
        super().__init__(
            source=source,
            name=source.name,
            data_type=source.data_type,
            var_type=source.var_type,
            on_null=source.on_null,
            is_event_time=source.is_event_time,
            is_primary_key=source.is_primary_key,
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
    ):
        super().__init__(
            name=name,
            args=list(args.values()),
            data_type=data_type,
            var_type=var_type,
            unit=unit,
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
    def __init__(self, name: str, source: LambdaFunction):
        super().__init__(
            name=name,
            source=source,
            data_type=source.data_type,
            var_type=source.var_type,
            is_event_time=False,
            is_primary_key=False,
            on_null="PASS",
        )


class DataFrame(lineage._Table):
    def __init__(self, name: str, source: lineage._Table, columns: lineage.ColumnList):
        if any(
            [
                type(column) not in [DataFrameColumn, LambdaOutput]
                for column in columns.list_all()
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
                    for column_name in source.primary_key.list_names()
                ]
            ),
            event_time=event_time,
        )
