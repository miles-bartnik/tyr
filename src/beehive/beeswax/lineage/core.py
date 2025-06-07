import copy
import os.path

import sqlparse
from typing import List, Any, Dict
import pandas as pd
import networkx as nx
import re
import units


class _Operator:
    """
    Mathematical, text, sql operations

    :param name: str - Operator name
    """

    def __init__(self, name: str) -> None:
        self.name = name

    def _node_data(self):
        return {
            "label": rf"Operator ID: {id(self)}",
            "name": str(self.name),
            "type": str(type(self)),
            "base": str(type(self).__bases__[0]),
        }

    def _outbound_edge_data(self):
        return {}

    def _inbound_edge_data(self):
        return {}


class _Expression:
    """
    Mathematical, text, SQL expressions

    :param left:
    :param right:
    :param operator: lineage.core._Operator
    :param on_null: str - "PASS"/"WARN"/"FAIL"
    :param is_primary_key: bool = False
    :param is_event_time: bool = False
    """

    def __init__(
        self,
        left,
        right,
        operator: _Operator,
        on_null: str = "PASS",
        is_primary_key: bool = False,
        is_event_time: bool = False,
    ):
        self.name = operator
        self.left = left
        self.right = right
        self.operator = operator
        self.on_null = on_null
        self.is_primary_key = is_primary_key
        self.is_event_time = is_event_time

    def _node_data(self):
        return {
            "label": rf"Expression ID: {id(self)}",
            "type": str(type(self)),
            "base": str(type(self).__bases__[0]),
        }

    def _outbound_edge_data(self):
        return {}

    def _inbound_edge_data(self):
        return {}


class _Column:
    """
    Column lineage object

    :param source:
    :param name: str
    :param data_type: lineage.values.Datatype
    :param var_type: str - "categorical"/"numeric"/"key"
    :param on_null: str - "PASS"/"WARN"/"FAIL"
    :param is_primary_key: - bool = False
    :param is_event_time: - bool = False
    """

    def __init__(
        self,
        source,
        name,
        data_type,
        var_type,
        on_null,
        is_primary_key,
        is_event_time,
    ) -> None:
        self.source = source
        self.name = name
        self.data_type = data_type
        self.var_type = var_type
        self.on_null = on_null
        self.is_primary_key = is_primary_key
        self.is_event_time = is_event_time

        if type(source) is ColumnList:
            self.unit = source.list_all()[0].unit
        else:
            self.unit = source.unit

    def _node_data(self):
        if self.data_type:
            if type(self.data_type) is str:
                data_type = self.data_type
            else:
                data_type = self.data_type.name
        else:
            data_type = ""

        if self.var_type:
            var_type = self.var_type
        else:
            var_type = ""

        return {
            "label": self.name,
            "data_type": data_type,
            "var_type": var_type,
            "type": str(type(self)),
            "base": str(type(self).__bases__[0]),
            "unit": self.unit.name,
        }

    def _outbound_edge_data(self):
        return {}

    def _inbound_edge_data(self):
        return {
            "is_primary_key": str(self.is_primary_key),
            "is_event_time": str(self.is_event_time),
            "on_null": self.on_null,
        }


class _BlankColumn:
    def __init__(
        self,
        name,
        data_type,
        var_type,
        on_null,
        is_primary_key,
        is_event_time,
        unit,
    ) -> None:
        self.name = name
        self.data_type = data_type
        self.var_type = var_type
        self.on_null = on_null
        self.is_primary_key = is_primary_key
        self.is_event_time = is_event_time
        self.unit = unit
        self.current_table = None
        self.source_table = None

    def _node_data(self):
        if self.data_type:
            if type(self.data_type) is str:
                data_type = self.data_type
            else:
                data_type = self.data_type.name
        else:
            data_type = ""

        if self.var_type:
            var_type = self.var_type
        else:
            var_type = ""

        return {
            "label": self.name,
            "data_type": data_type,
            "var_type": var_type,
            "type": str(type(self)),
            "base": str(type(self).__bases__[0]),
            "unit": self.unit.name,
        }

    def _outbound_edge_data(self):
        return {}

    def _inbound_edge_data(self):
        return {
            "is_primary_key": str(self.is_primary_key),
            "is_event_time": str(self.is_event_time),
            "on_null": self.on_null,
        }


class ColumnList:
    """
    Stores columns and column order

    :param columns: List[lineage.core._Column]
    """

    def __init__(self, columns=None) -> None:
        if columns:
            if any(
                [
                    type(item).__bases__[0] is not _Column
                    for item in self.__dict__.values()
                ]
            ):
                raise ValueError("All columns must be _Column object")

            self.is_empty = False

        else:
            self.is_empty = True
            columns = []

        for column in columns:
            setattr(self, column.name, copy.deepcopy(column))

        self.order = [column.name for column in columns]

    def __getitem__(self, item):
        if type(item) is list:
            return [getattr(self, value) for value in item]
        else:
            return getattr(self, item)

    def list_all(self):
        if not self.is_empty:
            columns = [getattr(self, item) for item in self.order]
        else:
            columns = []

        return columns

    def list_names(self):
        if not self.is_empty:
            return self.order
        else:
            return []

    def add(self, column, override: bool = False):
        if type(column).__bases__[0] not in [_Column, _SourceColumn]:
            raise ValueError("_Column must be _Column or _SourceColumn object")

        if column.name in self.list_names() and not override:
            raise ValueError(rf"_Column '{column.name}' exists in ColumnList")

        setattr(self, column.name, column)
        self.is_empty = False
        self.order = self.order + [column.name]


class OrderBy:
    """
    Columns and direction to order a table or partition

    :param columns: lineage.core.ColumnList
    :param how: List[lineage.core._Operator] - Operator must be in [lineage.operators.Ascending, lineage.operators.Descending]
    """

    def __init__(self, columns: ColumnList, how: List[_Operator] = []):
        if len(columns.list_names()) != len(how):
            raise ValueError("how:List[str] must have same length as columns")

        self.columns = columns
        self.how = how


class PartitionBy(ColumnList):
    """
    Columns and direction to partition a table or function

    :param columns: lineage.core.ColumnList
    """

    def __init__(self, columns: ColumnList):
        super().__init__(columns=columns.list_all())


class _Function:
    """
    Function lineage object

    :param name: str - Function name
    :param args: List[Any] - Function arguments in order
    :param data_type: lineage.values.Datatype - Data type of output
    :param var_type: str - "categorical"/"numeric"/"key"
    :param partition_by: lineage.core.PartitionBy = lineage.core.PartitionBy(ColumnList([]))
    :param order_by: lineage.core.OrderBy = lineage.core.OrderBy(columns=ColumnList([]), how=[])
    :param unit: lineage.units.core.Unit = lineage.units.core.Unit() - Unit of output
    """

    def __init__(
        self,
        name: str,
        args: List[Any],
        data_type=None,
        var_type: str = None,
        partition_by: PartitionBy = PartitionBy(ColumnList([])),
        order_by: OrderBy = OrderBy(columns=ColumnList([]), how=[]),
        unit: units.core.Unit = units.core.Unit(),
        distinct: bool = False,
    ) -> None:
        self.name = name
        self.args = args
        self.data_type = data_type
        self.var_type = var_type
        self.partition_by = partition_by
        self.order_by = order_by
        self.is_primary_key = False
        self.is_event_time = False
        self.unit = unit
        self.distinct = distinct

    def _node_data(self):
        if self.data_type:
            if type(self.data_type) is str:
                data_type = self.data_type
            else:
                data_type = self.data_type.name
        else:
            data_type = ""

        if self.var_type:
            var_type = self.var_type
        else:
            var_type = ""

        return {
            "label": self.name,
            "data_type": data_type,
            "var_type": var_type,
            "type": str(type(self)),
            "base": str(type(self).__bases__[0]),
            "unit": self.unit.name,
        }

    def _outbound_edge_data(self):
        return {}

    def _inbound_edge_data(self):
        return {}

    def _override_unit(self, unit):
        self.unit = unit
        return self


class _Value:
    """
    Value lineage object

    :param value: Value
    :param data_type: lineage.values.Datatype - Data type of value
    :param var_type: str - "categorical"/"numeric"/"key"
    :param unit: lineage.units.core.Unit = lineage.units.core.Unit() - Unit of value
    """

    def __init__(
        self,
        value,
        data_type,
        var_type: str = None,
        unit: units.core.Unit = units.core.Unit(),
    ) -> None:
        self.value = value
        self.name = value
        self.data_type = data_type
        self.var_type = var_type
        self.unit = unit

    def _node_data(self):
        if self.data_type:
            if type(self.data_type) is str:
                data_type = self.data_type
            else:
                data_type = self.data_type.name
        else:
            data_type = ""

        if self.var_type:
            var_type = self.var_type
        else:
            var_type = ""

        if len(str(self.name)) > 25:
            label = data_type
        else:
            label = self.name

        return {
            "label": label,
            "data_type": data_type,
            "var_type": var_type,
            "type": str(type(self)),
            "base": str(type(self).__bases__[0]),
            "unit": self.unit.name,
        }

    def _outbound_edge_data(self):
        return {}

    def _inbound_edge_data(self):
        return {}


class _SourceColumn:
    """
    Source column from column metadata

    :param column_metadata: pd.DataFrame - See column_metadata documentation
    """

    def __init__(self, column_metadata: pd.DataFrame):
        self.dataset = column_metadata["dataset"].iloc[0]
        self.source_name = column_metadata["column_name"].iloc[0]

        if column_metadata["column_alias"].iloc[0]:
            self.name = column_metadata["column_alias"].iloc[0]
        else:
            self.name = column_metadata["column_name"].iloc[0]

        self.var_type = column_metadata["var_type"].iloc[0]
        self.data_type = column_metadata["data_type"].iloc[0]
        self.on_null = column_metadata["on_null"].iloc[0]
        self.is_primary_key = column_metadata["is_primary_key"].iloc[0]
        self.is_event_time = column_metadata["is_event_time"].iloc[0]
        self.filter_values = column_metadata["filter_values"].iloc[0]
        self.on_filter = column_metadata["on_filter"].iloc[0]
        self.regex = column_metadata["regex"].iloc[0]
        self.source_unit = units.core.Unit(column_metadata["source_unit"].iloc[0])

        if self.source_unit.name and not column_metadata["target_unit"].iloc[0]:
            self.target_unit = self.source_unit
        else:
            self.target_unit = units.core.Unit(column_metadata["target_unit"].iloc[0])

        self.unit = self.source_unit

        self.column_metadata = column_metadata

    def _node_data(self):
        if self.data_type:
            if type(self.data_type) is str:
                data_type = self.data_type
            else:
                data_type = self.data_type.name
        else:
            data_type = ""

        if self.var_type:
            var_type = self.var_type
        else:
            var_type = ""

        return {
            "label": self.name,
            "data_type": data_type,
            "var_type": var_type,
            "type": str(type(self)),
            "base": str(type(self).__bases__[0]),
            "unit": self.unit.name,
        }

    def _outbound_edge_data(self):
        return {}

    def _inbound_edge_data(self):
        return {
            "is_primary_key": str(self.is_primary_key),
            "is_event_time": str(self.is_event_time),
            "on_null": self.on_null,
        }


class _SourceFile:
    def __init__(self, dataset, file_regex, delim, columns: ColumnList) -> None:
        self.dataset = dataset
        self.file_regex = file_regex
        self.name = file_regex.value
        self.columns = columns
        self.delim = delim

        for column in self.columns.list_all():
            setattr(column, "current_table", self)

    def _node_data(self):
        return {
            "label": self.name,
            "data_type": self.file_regex.value,
            "dataset": self.dataset.value,
            "delim": self.delim.value,
            "type": str(type(self)),
            "base": str(type(self).__bases__[0]),
        }

    def _outbound_edge_data(self):
        return {}

    def _inbound_edge_data(self):
        return {}


class Condition:
    def __init__(self, checks: List[Any], link_operators=None) -> None:
        if not link_operators:
            link_operators = []
        self.checks = checks
        self.link_operators = link_operators

        if len(link_operators) != len(checks) - 1:
            raise ValueError(
                rf"len(link_operators)!=len(checks)-1 : {len(link_operators)}!={len(checks)-1}"
            )

    def _node_data(self):
        return {
            "label": rf"Condition ID: {id(self)}",
            "type": str(type(self)),
            "base": str(type(self).__bases__[0]),
        }

    def _outbound_edge_data(self):
        return {}

    def _inbound_edge_data(self):
        return {}


class CaseWhen:
    def __init__(
        self, conditions: List[Condition], values: List[Any], else_value=None
    ) -> None:
        self.name = "case_when"
        self.conditions = conditions
        self.values = values
        self.unit = self.values[0].unit

        if len(self.conditions) != len(self.values):
            raise ValueError("conditions and values must have the same length")

        self.else_value = else_value
        self.data_type = self.values[0].data_type
        self.var_type = None
        self.is_primary_key = False
        self.is_event_time = False

        if self.else_value:
            self.on_null = "WARN"
        else:
            self.on_null = "PASS"

    def _node_data(self):
        if self.unit.name:
            unit = self.unit.name
        else:
            unit = ""

        return {
            "label": rf"CaseWhen ID: {id(self)}",
            "type": str(type(self)),
            "base": str(type(self).__bases__[0]),
            "unit": unit,
        }

    def _outbound_edge_data(self):
        return {}

    def _inbound_edge_data(self):
        return {}


class Record:
    def __init__(self, values: dict):
        self.values = list(values.values())
        self.columns = ColumnList(values.keys())

    def _node_data(self):
        return {"type": str(type(self))}

    def _outbound_edge_data(self):
        return {}

    def _inbound_edge_data(self):
        return {}


class RecordList:
    def __init__(self, name: str, records: List[Record]):
        if any([type(record) is not Record for record in records]):
            raise ValueError("All records must be Record objects")

        if not all(
            [
                record.columns.list_names() == records[0].columns.list_names()
                for record in records
            ]
        ):
            raise ValueError("All columns must be the same for all records")

        self.name = name
        self.records = records
        self.columns = self.records[0].columns

    def _node_data(self):
        return {"type": str(type(self))}

    def _outbound_edge_data(self):
        return {}

    def _inbound_edge_data(self):
        return {}


class RecordGenerator:
    def __init__(
        self, name: str, generator: callable, n_records: int, generator_args: dict = {}
    ):
        if type(generator([1], generator_args).__next__()) is not Record:
            raise ValueError("generator must return Record object")

        self.name = name
        self.generator = generator([x for x in range(n_records)], generator_args)
        self.columns = generator([1], generator_args).__next__().columns

        for column in self.columns.list_all():
            setattr(column, "source_table", None)
            setattr(column, "current_table", None)

    def _node_data(self):
        return {"type": str(type(self))}

    def _outbound_edge_data(self):
        return {}

    def _inbound_edge_data(self):
        return {}


class _Table:
    def __init__(
        self,
        name,
        columns: ColumnList,
        source=None,
        primary_key: ColumnList = ColumnList([]),
        event_time=None,
        distinct: bool = False,
        group_by: bool = False,
        where_condition: Condition = None,
        having_condition: Condition = None,
        ctes=None,
        schema=None,
    ) -> None:
        self.name = name
        self.source = source
        self.columns = columns
        self.primary_key = primary_key
        self.event_time = event_time
        self.schema = schema

        if self.event_time:
            self.static_primary_key = ColumnList(
                [
                    column
                    for column in self.primary_key.list_all()
                    if column.name != self.event_time.name
                ]
            )
        else:
            self.static_primary_key = self.primary_key

        self.distinct = distinct
        self.group_by = group_by
        self.where_condition = where_condition
        self.having_condition = having_condition

        if not ctes:
            self.ctes = TableList([])
        else:
            if type(ctes) is not TableList:
                raise ValueError("ctes must be TableList")
            else:
                self.ctes = ctes

        for column in self.columns.list_all():
            setattr(column, "current_table", self)

        for column in self.primary_key.list_all():
            setattr(column, "current_table", self)

        for column in self.static_primary_key.list_all():
            setattr(column, "current_table", self)

        if self.event_time:
            setattr(self.event_time, "current_table", self)

    def _node_data(self):
        return {
            "label": self.name,
            "distinct": str(self.distinct),
            "group_by": str(self.group_by),
            "type": str(type(self)),
            "base": str(type(self).__bases__[0]),
        }

    def _outbound_edge_data(self):
        return {}

    def _inbound_edge_data(self):
        return {}

    def add_column(self, column):
        setattr(column, "current_table", self)
        self.columns.add(column)

    def add_columns(self, columns):
        for column in columns.list_all():
            self.add_column(column)

    def set_primary_key(self, primary_key: ColumnList):
        self.primary_key = primary_key

        for column in self.primary_key.list_all():
            setattr(column, "current_table", self)

        if self.event_time:
            self.static_primary_key = ColumnList(
                [
                    column
                    for column in self.primary_key.list_all()
                    if column.name != self.event_time.name
                ]
            )

    def set_event_time(self, event_time):
        self.event_time = event_time
        setattr(self.event_time, "current_table", self)

        if self.primary_key:
            self.static_primary_key = ColumnList(
                [
                    column
                    for column in self.primary_key.list_all()
                    if column.name != self.event_time.name
                ]
            )

        else:
            self.primary_key = self.event_time


class TableList:
    def __init__(self, tables: List[Any]) -> None:
        if any(
            [type(table).__bases__[0] not in [_Table, _SourceFile] for table in tables]
        ):
            raise ValueError("All tables must be _Table object")

        for table in tables:
            setattr(self, table.name, copy.deepcopy(table))

        if tables != []:
            self.is_empty = False

        else:
            self.is_empty = True

    def __getitem__(self, item):
        if type(item) is list:
            return [getattr(self, value) for value in item]
        else:
            return getattr(self, item)

    def list_all(self):
        if not self.is_empty:
            tables = [
                item
                for item in self.__dict__.values()
                if ((type(item).__bases__[0] is _Table) or (type(item) is _Table))
            ]
        else:
            tables = []

        return tables

    def list_names(self):
        if not self.is_empty:
            return [
                item.name
                for item in self.__dict__.values()
                if ((type(item).__bases__[0] is _Table) or (type(item) is _Table))
            ]
        else:
            return []

    def add(self, table, override: bool = False):
        if not ((type(table).__bases__[0] is _Table) or (type(table) is _Table)):
            raise ValueError("_Table must be _Table object")

        if table.name in self.list_names() and not override:
            raise ValueError(rf"_Table '{table.name}' exists in TableList")

        setattr(self, table.name, table)
        setattr(self, "is_empty", False)


class _Transformation:
    def __init__(self, name, source, args):
        self.name = name
        self.source = source
        self.args = args

    def _node_data(self):
        return {
            "label": self.name,
            "type": str(type(self)),
            "base": str(type(self).__bases__[0]),
        }

    def _outbound_edge_data(self):
        return {}

    def _inbound_edge_data(self):
        return {}


def init_file_metadata(
    path: str = None,
):
    if not path:
        if not "configurations" in os.listdir(os.getcwd()):
            os.mkdir("configurations")

        path = rf"{os.getcwd()}/configurations/file_metadata.tsv"

    pd.DataFrame(
        columns=[
            "schema",
            "dataset",
            "file_regex",
            "delim",
        ]
    ).to_csv(path, sep="\t", header=True, index=False)


def init_column_metadata(
    path: str = None,
    file_metadata: pd.DataFrame = pd.DataFrame(),
):
    if not path:
        if not "configurations" in os.listdir(os.getcwd()):
            os.mkdir("configurations")

        path = rf"{os.getcwd()}/configurations/column_metadata.tsv"

    column_metadata_df = pd.DataFrame(
        columns=[
            "schema",
            "dataset",
            "column_name",
            "column_alias",
            "var_type",
            "data_type",
            "on_null",
            "is_primary_key",
            "is_event_time",
            "filter_values",
            "on_filter",
            "regex",
            "source_unit",
            "target_unit",
            "precision",
            "ordinal_position",
        ]
    )

    if not file_metadata.empty:
        for index, row in file_metadata.iterrows():
            file = [
                "/".join(row["file_regex"].split("/")[:-1]) + "/" + file
                for file in os.listdir("/".join(row["file_regex"].split("/")[:-1]))
                if re.search(
                    row["file_regex"].replace(".", "\.").replace("*", ".*"),
                    "/".join(row["file_regex"].split("/")[:-1]) + "/" + file,
                )
            ][0]

            if row["delim"] == "t":
                delim = "\t"
            elif row["delim"] == "c":
                delim = ","
            else:
                delim = row["delim"]

            ext = row["file_regex"].split(".")[-1]

            if delim:
                df = pd.read_csv(file, delimiter=delim, nrows=10)
            elif ext == "json":
                df = pd.read_json(file, nrows=10)
            elif ext == "geojson":
                df = pd.read_json(file, nrows=10)
            else:
                df = pd.DataFrame

            columns = df.columns.tolist()

            column_metadata_df = pd.concat(
                [
                    column_metadata_df,
                    pd.DataFrame.from_records(
                        [
                            {
                                "schema": row["schema"],
                                "dataset": row["dataset"],
                                "column_name": columns[i],
                                "column_alias": None,
                                "var_type": None,
                                "data_type": None,
                                "on_null": "PASS",
                                "is_primary_key": False,
                                "is_event_time": False,
                                "filter_values": None,
                                "on_filter": "PASS",
                                "regex": None,
                                "source_unit": None,
                                "target_unit": None,
                                "precision": None,
                                "ordinal_position": i,
                            }
                            for i in range(len(columns))
                        ]
                    ),
                ],
                ignore_index=True,
            )

    column_metadata_df.to_csv(path, sep="\t", header=True, index=False)
