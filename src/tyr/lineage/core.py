import copy
import os.path
import rustworkx as rx

from typing import List, Any, Dict, AnyStr
import pandas as pd
import re
import units
from ..interpreter import Interpreter
from .. import network
import collections

_interpreters = {"beeswax_duckdb": Interpreter()}


class _Operator:
    """
    Mathematical, text, sql operations

    :param name: str - Operator name e.g. "=", "AND", ...
    """

    def __init__(self, name: str, macro_group: str = "") -> None:
        self.name = name
        self.sql = _interpreters["beeswax_duckdb"].to_sql(self)
        self._node_data = {
            "label": rf"Operator ID: {id(self)}",
            "name": str(self.name),
            "type": str(type(self)),
            "base": str(type(self).__bases__[0]),
            "macro_group": macro_group,
            "sql": self.sql,
        }

        graph = rx.PyDiGraph()
        graph.add_node(self._node_data)
        self.graph = graph


class _Expression:
    """
    Mathematical, text, SQL expressions applying an operator between a left and right parameter.
    e.g. 1 = 1 -> TRUE

    :param left:
    :param right:
    :param operator: lineage.core._Operator
    :param on_null: str - "PASS"/"WARN"/"SKIP"/"FAIL"
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
        macro_group: str = "",
    ):
        self.name = operator
        self.left = left
        self.right = right
        self.operator = operator
        self.on_null = on_null
        self.is_primary_key = is_primary_key
        self.is_event_time = is_event_time

        self.sql = _interpreters["beeswax_duckdb"].to_sql(self)
        self._node_data = {
            "label": rf"Expression ID: {id(self)}",
            "type": str(type(self)),
            "base": str(type(self).__bases__[0]),
            "macro_group": macro_group,
            "sql": self.sql,
        }

        left = rx.PyDiGraph()
        right = rx.PyDiGraph()

        left.add_node(self.left._node_data)
        right.add_node(self.right._node_data)

        left.add_child(0, self._node_data, {})
        right.add_child(0, self._node_data, {})

        graph = rx.union(left, right, merge_nodes=True, merge_edges=True)
        self.graph = graph


class _Column:
    """
    Column lineage object

    :param source: Source of column
    :type source: _Column or _Function
    :param name: Name of column
    :type name: str
    :param data_type: Data type of column
    :type data_type: lineage.values.Datatype
    :param var_type: Variable type - "categorical"/"numeric"/"key", default = :code:`None`
    :type var_type: str
    :param on_null: Behaviour on null value - "PASS"/"WARN"/"SKIP"/"FAIL", default = :code:`"Pass"`
    :type on_null: str
    :param is_primary_key: If the column is the part of the primary key of the table, default = :code:`False`
    :type is_primary_key: bool
    :param is_event_time: If the column is the event time of the table, default = :code:`False`
    :type is_event_time: bool


    :return: tyr.lineage.core._Column
    :rtype: _Column
    """

    def __init__(
        self,
        source,
        name,
        data_type,
        var_type,
        on_null="PASS",
        is_primary_key=False,
        is_event_time=False,
        macro_group: str = "",
    ) -> None:
        self.source = source
        self.name = name
        self.data_type = data_type
        self.var_type = var_type
        self.on_null = on_null
        self.is_primary_key = is_primary_key
        self.is_event_time = is_event_time
        self.current_table = None

        if isinstance(source, ColumnList):
            self.unit = source.list_columns()[0].unit
        else:
            self.unit = source.unit

        self.sql = _interpreters["beeswax_duckdb"].to_sql(self)

        if self.data_type:
            if isinstance(self.data_type, str):
                data_type = self.data_type
            else:
                data_type = self.data_type.name
        else:
            data_type = ""

        if self.var_type:
            var_type = self.var_type
        else:
            var_type = ""

        self._node_data = {
            "label": self.name,
            "name": self.name,
            "data_type": data_type,
            "var_type": var_type,
            "type": str(type(self)),
            "base": str(type(self).__bases__[0]),
            "unit": self.unit.name,
            "macro_group": macro_group,
            "sql": self.sql,
        }

        graph = rx.PyDiGraph()
        graph.add_node(self._node_data)

        graph.add_child(0, self.source._node_data, {})

        graph = rx.union(graph, self.source.graph, merge_nodes=True, merge_edges=True)

        self.graph = graph


class ColumnList:
    """
    Stores columns and column order

    :param columns: List of column objects
    :type columns: List[columns.Core|columns.Select|columns.Record]
    """

    def __init__(self, columns=None) -> None:
        if columns:
            if any([not isinstance(item, _Column) for item in self.__dict__.values()]):
                raise ValueError("All columns must be _Column object")

            self.is_empty = False

        else:
            self.is_empty = True
            columns = []

        if any(
            [
                x > 1
                for x in dict(
                    collections.Counter([column.name for column in columns]).items()
                ).values()
            ]
        ):
            raise ValueError(
                rf"Duplicate column names detected: {dict(collections.Counter([column.name for column in columns]).items())}"
            )

        for column in columns:
            setattr(self, column.name, copy.deepcopy(column))

        self.order = [column.name for column in columns]

    def __getitem__(self, item):
        if isinstance(item, list):
            return [getattr(self, value) for value in item]
        else:
            return getattr(self, item)

    def __add__(self, other):
        if isinstance(other, ColumnList):
            return ColumnList(self.list_columns() + other.list_columns())

    def list_columns(
        self, filter_regex: str = None, filter_unit: units.core.Unit = None
    ):
        if not self.is_empty:
            if filter_regex:
                columns = [
                    getattr(self, item)
                    for item in self.order
                    if re.match(filter_regex, item)
                ]
            else:
                columns = [getattr(self, item) for item in self.order]

            if filter_unit:
                columns = [
                    getattr(self, item.name)
                    for item in columns
                    if filter_unit == getattr(self, item.name).unit
                ]

        else:
            columns = []

        return columns

    def list_names(self, filter_regex: str = None, filter_unit: units.core.Unit = None):
        if not self.is_empty:
            if filter_regex:
                columns = [item for item in self.order if re.match(filter_regex, item)]
            else:
                columns = self.order

            if filter_unit:
                columns = [
                    item for item in columns if filter_unit == getattr(self, item).unit
                ]

            return columns

        else:
            return []

    def add(self, column, override: bool = False):
        if not isinstance(column, _Column):
            raise ValueError("_Column must be _Column object")

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
        self.sql = _interpreters["beeswax_duckdb"].to_sql(self)


class PartitionBy(ColumnList):
    """
    Columns and direction to partition a table or function

    :param columns: lineage.core.ColumnList
    """

    def __init__(self, columns: ColumnList):
        super().__init__(columns=columns.list_columns())


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
        macro_group: str = "",
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
        self.sql = _interpreters["beeswax_duckdb"].to_sql(self)

        if self.data_type:
            if isinstance(self.data_type, str):
                data_type = self.data_type
            else:
                data_type = self.data_type.name
        else:
            data_type = ""

        if self.var_type:
            var_type = self.var_type
        else:
            var_type = ""

        self._node_data = {
            "label": self.name,
            "data_type": data_type,
            "var_type": var_type,
            "type": str(type(self)),
            "base": str(type(self).__bases__[0]),
            "unit": self.unit.name,
            "macro_group": macro_group,
            "sql": self.sql,
        }

        graph = rx.PyDiGraph()
        graph.add_node(self._node_data)
        self.graph = graph


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
        macro_group: str = "",
    ) -> None:
        self.value = value
        self.name = value
        self.data_type = data_type
        self.var_type = var_type
        self.unit = unit
        self.sql = _interpreters["beeswax_duckdb"].to_sql(self)

        if self.data_type:
            if isinstance(self.data_type, str):
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

        self._node_data = {
            "label": label,
            "data_type": data_type,
            "var_type": var_type,
            "type": str(type(self)),
            "base": str(type(self).__bases__[0]),
            "unit": self.unit.name,
            "macro_group": macro_group,
            "sql": self.sql,
        }

        graph = rx.PyDiGraph()
        graph.add_node(self._node_data)
        self.graph = graph


class Condition:
    def __init__(
        self, checks: List[Any], link_operators=None, macro_group: str = ""
    ) -> None:
        if not link_operators:
            link_operators = []
        self.checks = checks
        self.link_operators = link_operators

        if len(link_operators) != len(checks) - 1:
            raise ValueError(
                rf"len(link_operators)!=len(checks)-1 : {len(link_operators)}!={len(checks)-1}"
            )
        self.sql = _interpreters["beeswax_duckdb"].to_sql(self)

        self._node_data = {
            "label": rf"Condition ID: {id(self)}",
            "type": str(type(self)),
            "base": str(type(self).__bases__[0]),
            "macro_group": macro_group,
            "sql": self.sql,
        }

        graph = rx.PyDiGraph()
        graph.add_node(self._node_data)

        self.graph = graph


class CaseWhen:
    def __init__(
        self,
        conditions: List[Condition],
        values: List[Any],
        else_value=None,
        macro_group: str = "",
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
        self.sql = _interpreters["beeswax_duckdb"].to_sql(self)

        if self.unit.name:
            unit = self.unit.name
        else:
            unit = ""

        self._node_data = {
            "label": rf"CaseWhen ID: {id(self)}",
            "type": str(type(self)),
            "base": str(type(self).__bases__[0]),
            "unit": unit,
            "macro_group": macro_group,
            "sql": self.sql,
        }

        graph = rx.PyDiGraph()
        graph.add_node(self._node_data)
        self.graph = graph


class _Blank:
    def __init__(
        self,
        name,
        data_type,
        var_type: str = None,
        macro_group: str = None,
        unit: units.core.Unit = units.core.Unit(),
    ):
        self.name = name
        self.data_type = data_type
        self.var_type = var_type
        self.macro_group = macro_group
        self.unit = unit
        self.sql = _interpreters["beeswax_duckdb"].to_sql(self)

        if self.data_type:
            if isinstance(self.data_type, str):
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

        self._node_data = {
            "label": label,
            "data_type": data_type,
            "var_type": var_type,
            "type": str(type(self)),
            "base": str(type(self).__bases__[0]),
            "unit": self.unit.name,
            "macro_group": macro_group,
            "sql": self.sql,
        }


class Record:
    def __init__(self, values: dict, macro_group: str = ""):
        self.values = list(values.values())
        self.columns = ColumnList(values.keys())
        self.sql = _interpreters["beeswax_duckdb"].to_sql(self)

        self._node_data = {
            "type": str(type(self)),
            "macro_group": macro_group,
            "sql": self.sql,
        }

        graph = rx.PyDiGraph()
        graph.add_node(self._node_data)


class RecordList:
    def __init__(self, name: str, records: List[Record], macro_group: str = ""):
        if any([not isinstance(record, Record) for record in records]):
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
        self.sql = _interpreters["beeswax_duckdb"].to_sql(self)

        self._node_data = {
            "type": str(type(self)),
            "macro_group": macro_group,
            "sql": self.sql,
        }

        graph = rx.PyDiGraph()
        graph.add_node(self._node_data)
        self.graph = graph


class RecordGenerator:
    def __init__(
        self,
        name: str,
        generator: callable,
        n_records: int,
        generator_args: dict = {},
        macro_group: str = "",
    ):
        if not isinstance(generator([1], generator_args).__next__(), Record):
            raise ValueError("generator must return Record object")

        self.name = name
        self.generator = generator([x for x in range(n_records)], generator_args)
        self.columns = generator([1], generator_args).__next__().columns

        for column in self.columns.list_columns():
            setattr(column, "source_table", None)
            setattr(column, "current_table", None)
        self.sql = _interpreters["beeswax_duckdb"].to_sql(self)
        self._node_data = {
            "type": str(type(self)),
            "macro_group": macro_group,
            "sql": self.sql,
        }

        graph = rx.PyDiGraph()
        graph.add_node(self._node_data)
        self.graph = graph


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
        macro_group: str = "",
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
                    for column in self.primary_key.list_columns()
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
            if not isinstance(ctes, TableList):
                raise ValueError("ctes must be TableList")
            else:
                self.ctes = ctes

        for column in self.columns.list_columns():
            setattr(column, "current_table", self)
            setattr(column, "sql", _interpreters["beeswax_duckdb"].to_sql(column))

        for column in self.primary_key.list_columns():
            setattr(column, "current_table", self)
            setattr(column, "sql", _interpreters["beeswax_duckdb"].to_sql(column))

        for column in self.static_primary_key.list_columns():
            setattr(column, "current_table", self)
            setattr(column, "sql", _interpreters["beeswax_duckdb"].to_sql(column))

        if self.event_time:
            setattr(self.event_time, "current_table", self)
            setattr(
                event_time, "sql", _interpreters["beeswax_duckdb"].to_sql(event_time)
            )

        self.sql = _interpreters["beeswax_duckdb"].to_sql(self)

        self._node_data = {
            "label": self.name,
            "name": self.name,
            "distinct": str(self.distinct),
            "group_by": str(self.group_by),
            "type": str(type(self)),
            "base": str(type(self).__bases__[0]),
            "macro_group": macro_group,
            "sql": self.sql,
        }

        graph = rx.PyDiGraph()
        graph.add_node(self._node_data)

        if isinstance(self.source, TableList):
            for table in self.source.list_tables():
                graph.add_child(0, table._node_data, {})
                graph = rx.union(graph, table.graph, merge_nodes=True, merge_edges=True)
        else:
            graph.add_child(0, self.source._node_data, {})

        for column in self.columns.list_columns():
            graph.add_child(
                0,
                column._node_data,
                {
                    "is_primary_key": column.is_primary_key,
                    "is_event_time": column.is_event_time,
                },
            )
            graph = rx.union(graph, column.graph, merge_nodes=True, merge_edges=True)

        self.graph = graph

    def update_graph(self):
        graph = rx.PyDiGraph()
        graph.add_node(self._node_data)
        graph.add_child(0, self.source, {})

        for column in self.columns.list_columns():
            graph.add_child(
                0,
                column._node_data,
                {
                    "is_primary_key": column.is_primary_key,
                    "is_event_time": column.is_event_time,
                },
            )

        self.graph = graph

    def add_column(self, column):
        setattr(column, "current_table", self)
        self.columns.add(column)
        setattr(column, "sql", _interpreters["beeswax_duckdb"].to_sql(column))
        self.sql = _interpreters["beeswax_duckdb"].to_sql(self)

        self.graph.add_child(
            0,
            column._node_data,
            {
                "is_primary_key": column.is_primary_key,
                "is_event_time": column.is_event_time,
            },
        )

    def add_columns(self, columns):
        for column in columns.list_columns():
            self.add_column(column)
            setattr(column, "sql", _interpreters["beeswax_duckdb"].to_sql(column))
        self.sql = _interpreters["beeswax_duckdb"].to_sql(self)

    def set_primary_key(self, primary_key: ColumnList):
        for column in self.columns.list_columns():
            setattr(column, "is_primary_key", False)

        self.primary_key = primary_key

        for column in self.primary_key.list_columns():
            setattr(column, "current_table", self)
            setattr(column, "sql", _interpreters["beeswax_duckdb"].to_sql(column))

        if self.event_time:
            self.static_primary_key = ColumnList(
                [
                    column
                    for column in self.primary_key.list_columns()
                    if column.name != self.event_time.name
                ]
            )
        self.sql = _interpreters["beeswax_duckdb"].to_sql(self)

        self.update_graph()

    def set_event_time(self, event_time):
        for column in self.columns.list_columns():
            setattr(column, "is_event_time", False)

        self.event_time = event_time
        setattr(self.event_time, "current_table", self)
        setattr(event_time, "sql", _interpreters["beeswax_duckdb"].to_sql(event_time))

        if self.primary_key:
            self.static_primary_key = ColumnList(
                [
                    column
                    for column in self.primary_key.list_columns()
                    if column.name != self.event_time.name
                ]
            )

        else:
            self.primary_key = self.event_time
        self.sql = _interpreters["beeswax_duckdb"].to_sql(self)

        self.update_graph()


class TableList:
    def __init__(self, tables: List[Any]) -> None:
        if any([not isinstance(table, _Table) for table in tables]):
            raise ValueError("All tables must be _Table object")

        if any(
            [
                x > 1
                for x in dict(
                    collections.Counter([table.name for table in tables]).items()
                ).values()
            ]
        ):
            raise ValueError(
                rf"Duplicate table names detected: {dict(collections.Counter([table.name for table in tables]).items())}"
            )

        for table in tables:
            setattr(self, table.name, copy.deepcopy(table))

        if tables != []:
            self.is_empty = False

        else:
            self.is_empty = True

    def __getitem__(self, item):
        if isinstance(item, list):
            return [getattr(self, value) for value in item]
        else:
            return getattr(self, item)

    def list_tables(self):
        if not self.is_empty:
            tables = [
                item for item in self.__dict__.values() if isinstance(item, _Table)
            ]
        else:
            tables = []

        return tables

    def list_names(self):
        if not self.is_empty:
            return [
                item.name for item in self.__dict__.values() if isinstance(item, _Table)
            ]
        else:
            return []

    def add(self, table, override: bool = False):
        if not isinstance(table, _Table):
            raise ValueError("table must be _Table object")

        if table.name in self.list_names() and not override:
            raise ValueError(rf"_Table '{table.name}' exists in TableList")

        setattr(self, table.name, table)
        setattr(self, "is_empty", False)


class _Transformation:
    def __init__(self, name, source, args, macro_group: str = ""):
        self.name = name
        self.source = source
        self.args = args
        self.sql = _interpreters["beeswax_duckdb"].to_sql(self)

        self._node_data = {
            "label": self.name,
            "type": str(type(self)),
            "base": str(type(self).__bases__[0]),
            "macro_group": macro_group,
            "sql": self.sql,
        }

        graph = rx.PyDiGraph()
        graph.add_node(self._node_data)

        graph.add_child(0, self.source._node_data, {})

        self.graph = graph


class Window:
    def __init__(self):
        pass
