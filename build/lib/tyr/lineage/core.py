"""
Core intro
"""

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
import sqlparse

_interpreters = {"beeswax_duckdb": Interpreter()}


class LineageGraph:
    def __init__(self, rx_graph: rx.PyDiGraph):
        self.rx_graph = rx_graph

    def save(self, filename):
        rx.write_graphml(self.rx_graph, filename)

    def add_node(self, node_data):
        self.rx_graph.add_node(node_data)

    def add_edge(self, start: int, end: int, data: Dict = {}):
        self.rx_graph.add_edge(start, end, data)

    def add_child(self, parent, node_data, edge_data: Dict = {}):
        self.rx_graph.add_child(parent, node_data, edge_data)

    def add_parent(self, child, node_data, edge_data: Dict = {}):
        self.rx_graph.add_node(node_data)
        self.rx_graph.add_edge(
            self.node_index_from_data(node_data),
            child,
            edge_data,
        )

    def union(self, graphs: List):
        for graph in graphs:
            if isinstance(graph, type(self)):
                try:
                    self.rx_graph = rx.union(
                        self.rx_graph,
                        graph.rx_graph,
                        merge_nodes=True,
                        merge_edges=True,
                    )
                except:
                    print("Error encountered")
                    print(type(graph))
                    print(graph.__dict__)
                    raise TypeError("COULD NOT UNION graph")

            else:
                raise TypeError(rf"Graph object is not LineageGraph: {type(graph)}")

    def node_index_from_data(self, node_data):
        for index in self.rx_graph.node_indices():
            data = self.rx_graph.get_node_data(index)
            if data == node_data:
                return index
            else:
                pass

        return None


class _Operator:
    """
    Mathematical, text, sql operations

    :param name: Operator name in SQL e.g. "=", "AND", ...
    :type name: str
    """

    def __init__(self, name: str, macro_group: str = "") -> None:
        self.name = name
        self.sql = sqlparse.format(
            _interpreters["beeswax_duckdb"].to_sql(self), reindent=True
        )
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
        self.graph = LineageGraph(rx_graph=graph)


class _Expression:
    """
    Mathematical, text, SQL expressions applying an operator between a left and right parameter.
    e.g. 1 = 1 -> TRUE

    :param left:
    :type right: _Column|_Value
    :param right:
    :type right: _Column|_Value
    :param operator: Operator to apply
    :type operator: _Operator
    :param on_null: Behaviour to apply on result of expression - Options: ``"PASS"``/``"WARN"``/``"SKIP"``/``"FAIL"``, Default: ``"PASS""``
    :type on_null: str
    :param is_primary_key:  Default: ``False``
    :type is_primary_key: Default: ``bool``
    :param is_event_time: Default: ``False``
    :type is_event_time: bool
    :param macro_group: Used to group multiple pre-fabricated lineage objects into the same custom node collection - Default: ``""``
    :type macro_group: str
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

        self.sql = sqlparse.format(
            _interpreters["beeswax_duckdb"].to_sql(self), reindent=True
        )
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
        self.graph = LineageGraph(rx_graph=graph)

    def root_graph(self):
        graph = self.graph

        graph.union(
            [
                self.left.root_graph(),
                self.right.root_graph(),
            ]
        )

        return graph


class _Column:
    """
    Column lineage object

    :param source: Source of column
    :type source: _Column or _Function. The only exception to this is WildCard
    :param name: Name of column
    :type name: str
    :param data_type: Data type of column
    :type data_type: lineage.values.Datatype
    :param var_type: Variable type - "categorical"/"numeric"/"key", Default: ``None``
    :type var_type: str
    :param on_null: Behaviour on null value - "PASS"/"WARN"/"SKIP"/"FAIL", Default: ``"Pass"``
    :type on_null: str
    :param is_primary_key: If the column is the part of the primary key of the table, Default: ``False``
    :type is_primary_key: bool
    :param is_event_time: If the column is the event time of the table, Default: ``False``
    :type is_event_time: bool
    :param macro_group: Used to group multiple pre-fabricated lineage objects into the same custom node collection - Default: ``""``
    :type macro_group: str

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
            self.unit = source.list_columns_()[0].unit
        else:
            self.unit = source.unit

        self.sql = sqlparse.format(
            _interpreters["beeswax_duckdb"].to_sql(self), reindent=True
        )

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
        graph.add_node(self.source._node_data)
        graph = LineageGraph(graph)

        graph.add_child(0, self._node_data, {})

        self.graph = graph

    def root_graph(self):
        graph = self.graph

        graph.union([self.source.root_graph()])

        return graph


class ColumnList:
    """
    Stores columns and column order.
    This object is necessary for the following reasons:
     - Input validation of columns
     - Accessibility of columns as attributes of a table
     - Ease of manipulation of multiple column objects
     - Preserving lineage between columns and tables

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
            return ColumnList(self.list_columns_() + other.list_columns_())

    def list_columns_(
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

    def list_names_(
        self, filter_regex: str = None, filter_unit: units.core.Unit = None
    ):
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

    def add_(self, column, override: bool = False):
        if not isinstance(column, _Column):
            raise ValueError("_Column must be _Column object")

        if column.name in self.list_names_() and not override:
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
        if len(columns.list_names_()) != len(how):
            raise ValueError("how:List[str] must have same length as columns")

        self.columns = columns
        self.how = how
        self.sql = sqlparse.format(
            _interpreters["beeswax_duckdb"].to_sql(self), reindent=True
        )


class PartitionBy(ColumnList):
    """
    PartitionBy is a specific subclass of ColumnList for the purpose of creating partitions.
    It has been made distinct for the following reasons:
        - Ease of maintainability and avoiding confusion between ColumnList
        - Different treatment of partition columns within the lineage graph system

    :param columns: lineage.core.ColumnList
    """

    def __init__(self, columns: ColumnList):
        super().__init__(columns=columns.list_columns_())
        self.sql = sqlparse.format(
            _interpreters["beeswax_duckdb"].to_sql(self), reindent=True
        )


class _Value:
    """
    Value lineage object

    :param value: Value
    :param data_type: lineage.values.Datatype - Data type of value
    :param var_type: str - "categorical"/"numeric"/"key"
    :param unit: lineage.units.core.Unit = lineage.units.core.Unit() - Unit of value
    :param macro_group: Used to group multiple pre-fabricated lineage objects into the same custom node collection - Default: ``""``
    :type macro_group: str
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
        self.sql = sqlparse.format(
            _interpreters["beeswax_duckdb"].to_sql(self), reindent=True
        )

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
            label = str(data_type)
        else:
            label = str(self.name)

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
        self.graph = LineageGraph(rx_graph=graph)

    def root_graph(self):
        return self.graph


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
    :param macro_group: Used to group multiple pre-fabricated lineage objects into the same custom node collection - default value [""]
    :type macro_group: str
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
        framing: _Expression = None,
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
        self.framing = framing

        self.sql = sqlparse.format(
            _interpreters["beeswax_duckdb"].to_sql(self), reindent=True
        )

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
        self.graph = LineageGraph(rx_graph=graph)

        for arg in [arg for arg in self.args if "_node_data" in dir(arg)]:
            self.graph.add_parent(0, arg._node_data, edge_data={"type": "arg"})

    def root_graph(self):
        graph = self.graph

        graph.union([arg.root_graph() for arg in self.args])

        return graph


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
        self.sql = sqlparse.format(
            _interpreters["beeswax_duckdb"].to_sql(self), reindent=True
        )

        self._node_data = {
            "label": rf"Condition ID: {id(self)}",
            "type": str(type(self)),
            "base": str(type(self).__bases__[0]),
            "macro_group": macro_group,
            "sql": self.sql,
        }

        self.graph = LineageGraph(rx.PyDiGraph())

        if self.link_operators:
            check_graphs = []

            for i in range(len(self.link_operators)):
                check_graph = LineageGraph(rx.PyDiGraph())
                check_graph.add_node(self.checks[i]._node_data)
                check_graph.add_child(0, self.link_operators[i]._node_data)
                check_graph.add_parent(1, self.checks[i + 1]._node_data)
                check_graph.add_child(1, self._node_data)
                check_graphs.append(check_graph)

            self.graph = self.graph.union(check_graphs)

        else:
            self.graph.add_node(checks[0]._node_data)
            self.graph.add_child(0, self._node_data)

    def root_graph(self):
        graph = self.graph

        graph.union([LineageGraph(check.root_graph()) for check in self.checks])

        return graph


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
        self.sql = sqlparse.format(
            _interpreters["beeswax_duckdb"].to_sql(self), reindent=True
        )

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
        self.graph = LineageGraph(rx_graph=graph)


class _Blank:
    def __init__(
        self,
        name,
        data_type,
        var_type: str = None,
        macro_group: str = None,
        unit: units.core.Unit = units.core.Unit(),
        on_null: str = "PASS",
        is_primary_key: bool = False,
        is_event_time: bool = False,
    ):
        self.name = name
        self.data_type = data_type
        self.var_type = var_type
        self.macro_group = macro_group
        self.unit = unit
        self.sql = sqlparse.format(
            _interpreters["beeswax_duckdb"].to_sql(self), reindent=True
        )

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
        self.sql = sqlparse.format(
            _interpreters["beeswax_duckdb"].to_sql(self), reindent=True
        )

        self._node_data = {
            "type": str(type(self)),
            "macro_group": macro_group,
            "sql": self.sql,
        }

        graph = rx.PyDiGraph()
        graph.add_node(self._node_data)
        self.graph = LineageGraph(rx_graph=graph)


class RecordList:
    def __init__(self, name: str, records: List[Record], macro_group: str = ""):
        if any([not isinstance(record, Record) for record in records]):
            raise ValueError("All records must be Record objects")

        if not all(
            [
                record.columns.list_names_() == records[0].columns.list_names_()
                for record in records
            ]
        ):
            raise ValueError("All columns must be the same for all records")

        self.name = name
        self.records = records
        self.columns = self.records[0].columns
        self.sql = sqlparse.format(
            _interpreters["beeswax_duckdb"].to_sql(self), reindent=True
        )

        self._node_data = {
            "type": str(type(self)),
            "macro_group": macro_group,
            "sql": self.sql,
        }

        graph = rx.PyDiGraph()
        graph.add_node(self._node_data)
        self.graph = LineageGraph(rx_graph=graph)


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

        for column in self.columns.list_columns_():
            setattr(column, "source_table", None)
            setattr(column, "current_table", None)
        self.sql = sqlparse.format(
            _interpreters["beeswax_duckdb"].to_sql(self), reindent=True
        )
        self._node_data = {
            "type": str(type(self)),
            "macro_group": macro_group,
            "sql": self.sql,
        }

        graph = rx.PyDiGraph()
        graph.add_node(self._node_data)
        self.graph = LineageGraph(rx_graph=graph)


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
        order_by: OrderBy = None,
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
                    for column in self.primary_key.list_columns_()
                    if column.name != self.event_time.name
                ]
            )
        else:
            self.static_primary_key = self.primary_key

        self.distinct = distinct
        self.group_by = group_by
        self.where_condition = where_condition
        self.having_condition = having_condition
        self.order_by = order_by

        if not ctes:
            self.ctes = TableList([])
        else:
            if not isinstance(ctes, TableList):
                raise ValueError("ctes must be TableList")
            else:
                self.ctes = ctes

        for column in self.columns.list_columns_():
            setattr(column, "current_table", self)
            setattr(column, "sql", _interpreters["beeswax_duckdb"].to_sql(column))

        for column in self.primary_key.list_columns_():
            setattr(column, "current_table", self)
            setattr(column, "sql", _interpreters["beeswax_duckdb"].to_sql(column))

        for column in self.static_primary_key.list_columns_():
            setattr(column, "current_table", self)
            setattr(column, "sql", _interpreters["beeswax_duckdb"].to_sql(column))

        if self.event_time:
            setattr(self.event_time, "current_table", self)
            setattr(
                event_time, "sql", _interpreters["beeswax_duckdb"].to_sql(event_time)
            )

        self.sql = sqlparse.format(
            _interpreters["beeswax_duckdb"].to_sql(self), reindent=True
        )

        if self.schema:
            label = rf"{self.schema.settings.name}.{self.name}"
            schema = self.schema.settings.name
        else:
            label = self.name
            schema = ""

        self._node_data = {
            "label": label,
            "name": self.name,
            "schema": schema,
            "distinct": str(self.distinct),
            "group_by": str(self.group_by),
            "type": str(type(self)),
            "base": str(type(self).__bases__[0]),
            "macro_group": macro_group,
            "sql": self.sql,
        }

        graph = rx.PyDiGraph()
        graph.add_node(self._node_data)
        graph = LineageGraph(graph)

        if isinstance(self.source, TableList):
            for table in self.source.list_tables_():
                graph.add_parent(0, table._node_data, {})
        else:
            graph.add_parent(0, source._node_data, {})

        for column in self.columns.list_columns_():
            graph.add_child(
                0,
                column._node_data,
                {
                    "is_primary_key": column.is_primary_key,
                    "is_event_time": column.is_event_time,
                },
            )

        self.graph = graph

    def update_graph(self):
        if self.schema:
            self._node_data["label"] = rf"{self.schema.settings.name}.{self.name}"
            self._node_data["schema"] = self.schema.settings.name
        else:
            self._node_data["label"] = self.name
            self._node_data["schema"] = ""

        graph = rx.PyDiGraph()
        graph.add_node(self._node_data)
        graph.add_parent(0, self.source._node_data, {})

        for column in self.columns.list_columns_():
            graph.add_child(
                0,
                column._node_data,
                {
                    "is_primary_key": column.is_primary_key,
                    "is_event_time": column.is_event_time,
                },
            )

        self.graph = LineageGraph(rx_graph=graph)

    def add_column(self, column):
        setattr(column, "current_table", self)
        self.columns.add(column)
        setattr(column, "sql", _interpreters["beeswax_duckdb"].to_sql(column))
        self.sql = sqlparse.format(
            _interpreters["beeswax_duckdb"].to_sql(self), reindent=True
        )

        self.update_graph()

    def add_columns(self, columns):
        for column in columns.list_columns_():
            self.add_column(column)
            setattr(column, "sql", _interpreters["beeswax_duckdb"].to_sql(column))
        self.sql = sqlparse.format(
            _interpreters["beeswax_duckdb"].to_sql(self), reindent=True
        )

        self.update_graph()

    def set_primary_key(self, primary_key: ColumnList):
        for column in self.columns.list_columns_():
            setattr(column, "is_primary_key", False)

        self.primary_key = primary_key

        for column in self.primary_key.list_columns_():
            setattr(column, "current_table", self)
            setattr(column, "sql", _interpreters["beeswax_duckdb"].to_sql(column))

        if self.event_time:
            self.static_primary_key = ColumnList(
                [
                    column
                    for column in self.primary_key.list_columns_()
                    if column.name != self.event_time.name
                ]
            )
        self.sql = sqlparse.format(
            _interpreters["beeswax_duckdb"].to_sql(self), reindent=True
        )

        self.update_graph()

    def set_event_time(self, event_time):
        for column in self.columns.list_columns_():
            setattr(column, "is_event_time", False)

        self.event_time = event_time
        setattr(self.event_time, "current_table", self)
        setattr(event_time, "sql", _interpreters["beeswax_duckdb"].to_sql(event_time))

        if self.primary_key:
            self.static_primary_key = ColumnList(
                [
                    column
                    for column in self.primary_key.list_columns_()
                    if column.name != self.event_time.name
                ]
            )

        else:
            self.primary_key = self.event_time
        self.sql = sqlparse.format(
            _interpreters["beeswax_duckdb"].to_sql(self), reindent=True
        )

        self.update_graph()

    def root_graph(self):
        graph = self.graph

        graph.union([column.root_graph() for column in self.columns.list_columns_()])

        if type(self.source) is ColumnList:
            graph.union([table.root_graph() for table in self.source.list_tables_()])
        else:
            graph.union([self.source.root_graph()])

        return graph


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

    def list_tables_(self):
        if not self.is_empty:
            tables = [
                item for item in self.__dict__.values() if isinstance(item, _Table)
            ]
        else:
            tables = []

        return tables

    def list_names_(self):
        if not self.is_empty:
            return [
                item.name for item in self.__dict__.values() if isinstance(item, _Table)
            ]
        else:
            return []

    def add_(self, table, override: bool = False):
        if not isinstance(table, _Table):
            raise ValueError("table must be _Table object")

        if table.name in self.list_names_() and not override:
            raise ValueError(rf"_Table '{table.name}' exists in TableList")

        setattr(self, table.name, table)
        setattr(self, "is_empty", False)


class _Transformation:
    def __init__(self, name, source, args, macro_group: str = ""):
        self.name = name
        self.source = source
        self.args = args
        self.sql = sqlparse.format(
            _interpreters["beeswax_duckdb"].to_sql(self), reindent=True
        )

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

        self.graph = LineageGraph(rx_graph=graph)


class AppendOperator:

    """
    **AppendOperator** behaves similarly to an **Expression** object with only the right side.

    :param source: Object to apply operator to
    :type source: Any
    :param operator: Operator to apply
    :type operator: lineage._Operator
    """

    def __init__(self, source, operator, macro_group: str = ""):
        self.source = source
        self.operator = operator
        self.sql = sqlparse.format(
            _interpreters["beeswax_duckdb"].to_sql(self), reindent=True
        )

        self._node_data = {
            "label": rf"AppendOperator - {id(self)}",
            "type": str(type(self)),
            "base": str(type(self).__bases__[0]),
            "macro_group": macro_group,
            "sql": self.sql,
        }
        graph = rx.PyDiGraph()
        graph.add_node(self._node_data)

        graph.add_child(0, self.source._node_data, {})

        self.graph = LineageGraph(rx_graph=graph)
