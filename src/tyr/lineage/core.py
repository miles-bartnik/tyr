"""
Core intro
"""

import copy
import os.path
import rustworkx as rx

from typing import List, Any, Dict, AnyStr, OrderedDict
import pandas as pd
import re
from .units.core import Unit
from ..interpreter import Interpreter
import collections
import sqlparse


DATE_SPECIFIERS = [
    {
        "specifier": "%a",
        "description": "Abbreviated weekday name.",
        "example": "Sun, Mon, …",
        "interval_translate": {},
    },
    {
        "specifier": "%A",
        "description": "Full weekday name.",
        "example": "Sunday, Monday, …",
        "interval_translate": {},
    },
    {
        "specifier": "%b",
        "description": "Abbreviated month name.",
        "example": "Jan, Feb, …, Dec",
        "interval_translate": {},
    },
    {
        "specifier": "%B",
        "description": "Full month name.",
        "example": "January, February, …",
        "interval_translate": {},
    },
    {
        "specifier": "%c",
        "description": "ISO date and time representation",
        "example": "1992-03-02 10:30:20",
        "interval_translate": {},
    },
    {
        "specifier": "%d",
        "description": "Day of the month as a zero-padded decimal.",
        "example": "01, 02, …, 31",
        "interval_translate": {"regex": "\d{2}", "group_name": "days"},
    },
    {
        "specifier": "%-d",
        "description": "Day of the month as a decimal number.",
        "example": "1, 2, …, 30",
        "interval_translate": {"regex": "\d{1,2}", "group_name": "days"},
    },
    {
        "specifier": "%f",
        "description": "Microsecond as a decimal number, zero-padded on the left.",
        "example": "000000 - 999999",
        "interval_translate": {"regex": "(\d{6})", "group_name": "microseconds"},
    },
    {
        "specifier": "%g",
        "description": "Millisecond as a decimal number, zero-padded on the left.",
        "example": "000 - 999",
        "interval_translate": {"regex": "\d{3}", "group_name": "milliseconds"},
    },
    {
        "specifier": "%G",
        "description": "ISO 8601 year with century representing the year that contains the greater part of the ISO week (see %V).",
        "example": "0001, 0002, …, 2013, 2014, …, 9998, 9999",
        "interval_translate": {},
    },
    {
        "specifier": "%H",
        "description": "Hour (24-hour clock) as a zero-padded decimal number.",
        "example": "00, 01, …, 23",
        "interval_translate": {"regex": "\d{2}", "group_name": "hours"},
    },
    {
        "specifier": "%-H",
        "description": "Hour (24-hour clock) as a decimal number.",
        "example": "0, 1, …, 23",
        "interval_translate": {"regex": "\d{1,2}", "group_name": "hours"},
    },
    {
        "specifier": "%I",
        "description": "Hour (12-hour clock) as a zero-padded decimal number.",
        "example": "01, 02, …, 12",
        "interval_translate": {"regex": "\d{2}", "group_name": "hours"},
    },
    {
        "specifier": "%-I",
        "description": "Hour (12-hour clock) as a decimal number.",
        "example": "1, 2, … 12",
        "interval_translate": {"regex": "\d{1,2}", "group_name": "hours"},
    },
    {
        "specifier": "%j",
        "description": "Day of the year as a zero-padded decimal number.",
        "example": "001, 002, …, 366",
        "interval_translate": {"regex": "\d{3}", "group_name": "days"},
    },
    {
        "specifier": "%-j",
        "description": "Day of the year as a decimal number.",
        "example": "1, 2, …, 366",
        "interval_translate": {"regex": "\d{1,3}", "group_name": "days"},
    },
    {
        "specifier": "%m",
        "description": "Month as a zero-padded decimal number.",
        "example": "01, 02, …, 12",
        "interval_translate": {"regex": "\d{2}", "group_name": "months"},
    },
    {
        "specifier": "%-m",
        "description": "Month as a decimal number.",
        "example": "1, 2, …, 12",
        "interval_translate": {"regex": "\d{1,2}", "group_name": "months"},
    },
    {
        "specifier": "%M",
        "description": "Minute as a zero-padded decimal number.",
        "example": "00, 01, …, 59",
        "interval_translate": {"regex": "\d{2}", "group_name": "minutes"},
    },
    {
        "specifier": "%-M",
        "description": "Minute as a decimal number.",
        "example": "0, 1, …, 59",
        "interval_translate": {"regex": "\d{1,2}", "group_name": "minutes"},
    },
    {
        "specifier": "%n",
        "description": "Nanosecond as a decimal number, zero-padded on the left.",
        "example": "000000000 - 999999999",
        "interval_translate": {"regex": "\d{9}", "group_name": "nanoseconds"},
    },
    {
        "specifier": "%p",
        "description": "Locale's AM or PM.",
        "example": "AM, PM",
        "interval_translate": {},
    },
    {
        "specifier": "%S",
        "description": "Second as a zero-padded decimal number.",
        "example": "00, 01, …, 59",
        "interval_translate": {"regex": "\d{2}", "group_name": "seconds"},
    },
    {
        "specifier": "%-S",
        "description": "Second as a decimal number.",
        "example": "0, 1, …, 59",
        "interval_translate": {"regex": "\d{1,2}", "group_name": "seconds"},
    },
    {
        "specifier": "%u",
        "description": "ISO 8601 weekday as a decimal number where 1 is Monday.",
        "example": "1, 2, …, 7",
        "interval_translate": {},
    },
    {
        "specifier": "%U",
        "description": "Week number of the year. Week 01 starts on the first Sunday of the year, so there can be week 00. Note that this is not compliant with the week date standard in ISO-8601.",
        "example": "00, 01, …, 53",
        "interval_translate": {},
    },
    {
        "specifier": "%V",
        "description": "ISO 8601 week as a decimal number with Monday as the first day of the week. Week 01 is the week containing Jan 4. Note that %V is incompatible with year directive %Y. Use the ISO year %G instead.",
        "example": "01, …, 53",
        "interval_translate": {},
    },
    {
        "specifier": "%w",
        "description": "Weekday as a decimal number.",
        "example": "0, 1, …, 6",
        "interval_translate": {},
    },
    {
        "specifier": "%W",
        "description": "Week number of the year. Week 01 starts on the first Monday of the year, so there can be week 00. Note that this is not compliant with the week date standard in ISO-8601.",
        "example": "00, 01, …, 53",
        "interval_translate": {},
    },
    {
        "specifier": "%x",
        "description": "ISO date representation",
        "example": "1992-03-02",
        "interval_translate": {},
    },
    {
        "specifier": "%X",
        "description": "ISO time representation",
        "example": "10:30:20",
        "interval_translate": {},
    },
    {
        "specifier": "%y",
        "description": "Year without century as a zero-padded decimal number.",
        "example": "00, 01, …, 99",
        "interval_translate": {"regex": "\d{2}", "group_name": "years"},
    },
    {
        "specifier": "%-y",
        "description": "Year without century as a decimal number.",
        "example": "0, 1, …, 99",
        "interval_translate": {"regex": "\d{1,2}", "group_name": "years"},
    },
    {
        "specifier": "%Y",
        "description": "Year with century as a decimal number.",
        "example": "2013, 2019 etc.",
        "interval_translate": {"regex": "\d{4}", "group_name": "years"},
    },
    {
        "specifier": "%z",
        "description": "Time offset from UTC in the form ±HH:MM, ±HHMM, or ±HH.",
        "example": "-0700",
        "interval_translate": {},
    },
    {
        "specifier": "%Z",
        "description": "Time zone name.",
        "example": "Europe/Amsterdam",
        "interval_translate": {},
    },
    {
        "specifier": "%%",
        "description": "A literal % character.",
        "example": "%",
        "interval_translate": {},
    },
]


SUPPORTED_VARIABLE_TYPES = [
    "key",
    "numeric",
    "string",
    "datetime",
    "categorical",
    "interval",
    "timestamp",
    "sequential",
]

SUPPORTED_DATA_TYPES = {
    "Array": [
        {
            "extension": None,
            "data_type_regex": r"^(?P<data_type>ARRAY)$",
            "value_class": "Array",
            "unit": False,
        }
    ],
    "Bitstring": [
        {
            "extension": None,
            "data_type_regex": r"^(?P<data_type>BITSTRING)$",
            "value_class": "BitString",
            "unit": False,
        },
        {
            "extension": None,
            "data_type_regex": r"^(?P<data_type>BIT)$",
            "value_class": "BitString",
            "unit": False,
        },
    ],
    "Blob": [
        {
            "extension": None,
            "data_type_regex": r"^(?P<data_type>BYTEA)$",
            "value_class": "Blob",
            "unit": False,
        },
        {
            "extension": None,
            "data_type_regex": r"^(?P<data_type>BINARY)$",
            "value_class": "Blob",
            "unit": False,
        },
        {
            "extension": None,
            "data_type_regex": r"^(?P<data_type>VARBINARY)$",
            "value_class": "Blob",
            "unit": False,
        },
    ],
    "Boolean": [
        {
            "extension": None,
            "data_type_regex": r"^(?P<data_type>BOOLEAN)$",
            "value_class": "Boolean",
            "unit": False,
        },
        {
            "extension": None,
            "data_type_regex": r"^(?P<data_type>BOOL)$",
            "value_class": "Boolean",
            "unit": False,
        },
    ],
    "Date": [
        {
            "extension": None,
            "data_type_regex": r"^(?P<data_type>DATE)$",
            "value_class": "Date",
            "unit": False,
        }
    ],
    "Enum": [
        {
            "extension": None,
            "data_type_regex": r"^(?P<data_type>ENUM)$",
            "value_class": "Enum",
            "unit": False,
        }
    ],
    "Interval": [
        {
            "extension": None,
            "data_type_regex": r"^(?P<data_type>INTERVAL)$",
            "value_class": "Interval",
            "unit": True,
        }
    ],
    "Json": [
        {
            "extension": None,
            "data_type_regex": r"^(?P<data_type>JSON)$",
            "value_class": "JSON",
            "unit": False,
        }
    ],
    "Map": [
        {
            "extension": None,
            "data_type_regex": r"^(?P<data_type>MAP)$",
            "value_class": "Map",
            "unit": False,
        }
    ],
    "Numeric": [
        {
            "extension": None,
            "data_type_regex": r"^(?P<data_type>TINYINT)$",
            "value_class": "Integer",
            "unit": True,
        },
        {
            "extension": None,
            "data_type_regex": r"^(?P<data_type>SMALLINT)$",
            "value_class": "Integer",
            "unit": True,
        },
        {
            "extension": None,
            "data_type_regex": r"^(?P<data_type>SHORT)$",
            "value_class": "Integer",
            "unit": True,
        },
        {
            "extension": None,
            "data_type_regex": r"^(?P<data_type>INTEGER)$",
            "value_class": "Integer",
            "unit": True,
        },
        {
            "extension": None,
            "data_type_regex": r"^(?P<data_type>INT)$",
            "value_class": "Integer",
            "unit": True,
        },
        {
            "extension": None,
            "data_type_regex": r"^(?P<data_type>SIGNED)$",
            "value_class": "Integer",
            "unit": True,
        },
        {
            "extension": None,
            "data_type_regex": r"^(?P<data_type>BIGINT)$",
            "value_class": "Integer",
            "unit": True,
        },
        {
            "extension": None,
            "data_type_regex": r"^(?P<data_type>LONG)$",
            "value_class": "Integer",
            "unit": True,
        },
        {
            "extension": None,
            "data_type_regex": r"^(?P<data_type>HUGEINT)$",
            "value_class": "Integer",
            "unit": True,
        },
        {
            "extension": None,
            "data_type_regex": r"^(?P<data_type>UTINYINT)$",
            "value_class": "Integer",
            "unit": True,
        },
        {
            "extension": None,
            "data_type_regex": r"^(?P<data_type>USMALLINT)$",
            "value_class": "Integer",
            "unit": True,
        },
        {
            "extension": None,
            "data_type_regex": r"^(?P<data_type>UINTEGER)$",
            "value_class": "Integer",
            "unit": True,
        },
        {
            "extension": None,
            "data_type_regex": r"^(?P<data_type>UBIGINT)$",
            "value_class": "Integer",
            "unit": True,
        },
        {
            "extension": None,
            "data_type_regex": r"^(?P<data_type>UHUGEINT)$",
            "value_class": "Integer",
            "unit": True,
        },
        {
            "extension": None,
            "data_type_regex": r"^(?P<data_type>DECIMAL)$",
            "value_class": "Decimal",
            "unit": True,
        },
        {
            "extension": None,
            "data_type_regex": r"^(?P<data_type>DECIMAL)\((?P<width>\d+),\s?(?P<scale>\d+)\)$",
            "value_class": "Decimal",
            "unit": True,
        },
        {
            "extension": None,
            "data_type_regex": r"^(?P<data_type>INT)(?P<size>\d+)$",
            "value_class": "Integer",
            "unit": True,
        },
        {
            "extension": None,
            "data_type_regex": r"^(?P<data_type>UINT)(?P<size>\d+)$",
            "value_class": "Integer",
            "unit": True,
        },
        {
            "extension": None,
            "data_type_regex": r"^(?P<data_type>FLOAT)$",
            "value_class": "FloatingPoint",
            "unit": False,
        },
        {
            "extension": None,
            "data_type_regex": r"^(?P<data_type>FLOAT)(?P<size>\d+)$",
            "value_class": "FloatingPoint",
            "unit": False,
        },
        {
            "extension": None,
            "data_type_regex": r"^(?P<data_type>REAL)$",
            "value_class": "FloatingPoint",
            "unit": False,
        },
        {
            "extension": None,
            "data_type_regex": r"^(?P<data_type>DOUBLE)$",
            "value_class": "FloatingPoint",
            "unit": False,
        },
    ],
    "Struct": [
        {
            "extension": None,
            "data_type_regex": r"^(?P<data_type>STRUCT)$",
            "value_class": "Struct",
            "unit": False,
        }
    ],
    "Text": [
        {
            "extension": None,
            "data_type_regex": r"^(?P<data_type>VARCHAR)$",
            "value_class": "Varchar",
            "unit": False,
        },
        {
            "extension": None,
            "data_type_regex": r"^(?P<data_type>CHAR)$",
            "value_class": "Varchar",
            "unit": False,
        },
        {
            "extension": None,
            "data_type_regex": r"^(?P<data_type>BPCHAR)$",
            "value_class": "Varchar",
            "unit": False,
        },
        {
            "extension": None,
            "data_type_regex": r"^(?P<data_type>STRING)$",
            "value_class": "Varchar",
            "unit": False,
        },
        {
            "extension": None,
            "data_type_regex": r"^(?P<data_type>TEXT)$",
            "value_class": "Varchar",
            "unit": False,
        },
        {
            "extension": None,
            "data_type_regex": r"^(?P<data_type>VARCHAR)\((?P<size>\d+)\)$",
            "value_class": "Varchar",
            "unit": False,
        },
        {
            "extension": None,
            "data_type_regex": r"^(?P<data_type>CHAR)\((?P<size>\d+)\)$",
            "value_class": "Varchar",
            "unit": False,
        },
        {
            "extension": None,
            "data_type_regex": r"^(?P<data_type>BPCHAR)\((?P<size>\d+)\)$",
            "value_class": "Varchar",
            "unit": False,
        },
        {
            "extension": None,
            "data_type_regex": r"^(?P<data_type>STRING)\((?P<size>\d+)\)$",
            "value_class": "Varchar",
            "unit": False,
        },
        {
            "extension": None,
            "data_type_regex": r"^(?P<data_type>TEXT)\((?P<size>\d+)\)$",
            "value_class": "Varchar",
            "unit": False,
        },
    ],
    "Time": [
        {
            "extension": None,
            "data_type_regex": r"^(?P<data_type>TIME)$",
            "value_class": "Time",
            "unit": False,
        },
        {
            "extension": None,
            "data_type_regex": r"^(?P<data_type>TIME WITHOUT TIME ZONE)$",
            "value_class": "Time",
            "unit": False,
        },
        {
            "extension": None,
            "data_type_regex": r"^(?P<data_type>TIMETZ)$",
            "value_class": "Time",
            "unit": False,
        },
        {
            "extension": None,
            "data_type_regex": r"^(?P<data_type>TIME WITH TIME ZONE)$",
            "value_class": "Time",
            "unit": False,
        },
        {
            "extension": None,
            "data_type_regex": r"^(?P<data_type>TIME_NS)$",
            "value_class": "Time",
            "unit": False,
        },
    ],
    "Timestamp": [
        {
            "extension": None,
            "data_type_regex": r"^(?P<data_type>TIMESTAMP_NS)$",
            "value_class": "Timestamp",
            "unit": False,
        },
        {
            "extension": None,
            "data_type_regex": r"^(?P<data_type>TIMESTAMP)$",
            "value_class": "Timestamp",
            "unit": False,
        },
        {
            "extension": None,
            "data_type_regex": r"^(?P<data_type>DATETIME)$",
            "value_class": "Timestamp",
            "unit": False,
        },
        {
            "extension": None,
            "data_type_regex": r"^(?P<data_type>TIMESTAMP WITHOUT TIME ZONE)$",
            "value_class": "Timestamp",
            "unit": False,
        },
        {
            "extension": None,
            "data_type_regex": r"^(?P<data_type>TIMESTAMP_MS)$",
            "value_class": "Timestamp",
            "unit": False,
        },
        {
            "extension": None,
            "data_type_regex": r"^(?P<data_type>TIMESTAMP_S)$",
            "value_class": "Timestamp",
            "unit": False,
        },
        {
            "extension": None,
            "data_type_regex": r"^(?P<data_type>TIMESTAMPTZ)$",
            "value_class": "Timestamp",
            "unit": False,
        },
        {
            "extension": None,
            "data_type_regex": r"^(?P<data_type>TIMESTAMP WITH TIME ZONE)$",
            "value_class": "Timestamp",
            "unit": False,
        },
    ],
    "Wildcard": [
        {
            "extension": None,
            "data_type_regex": r"^(?P<data_type>WILDCARD)$",
            "value_class": "WildCard",
            "unit": False,
        }
    ],
    "Geometry": [
        {
            "extension": "spatial",
            "data_type_regex": r"^(?P<data_type>GEOMETRY)$",
            "value_class": "Geometry",
            "unit": False,
        }
    ],
}

SUPPORTED_DATA_TYPES["List"] = [
    {
        "extension": data_type["extension"],
        "data_type_regex": data_type["data_type_regex"].replace("\)$", r"\)\[\]$"),
        "value_class": "List",
        "unit": False,
    }
    if data_type["data_type_regex"][-3:] == "\)$"
    else {
        "extension": data_type["extension"],
        "data_type_regex": data_type["data_type_regex"].replace(")$", r")\[\]$"),
        "value_class": "List",
        "unit": False,
    }
    for key, value in SUPPORTED_DATA_TYPES.items()
    for data_type in value
    if key not in ["Wildcard"]
]

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

    def __init__(self, name: str) -> None:
        self.name = name
        self.sql = sqlparse.format(
            _interpreters["beeswax_duckdb"].to_sql(self), reindent=True
        )
        self._node_data = {
            "label": rf"Operator ID: {id(self)}",
            "name": str(self.name),
            "type": str(type(self)),
            "base": str(type(self).__bases__[0]),
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
    :type right: _Column|_Value|_Expression
    :param right:
    :type right: _Column|_Value|_Expression
    :param operator: Operator to apply
    :type operator: _Operator
    :param on_null: Behaviour to apply on result of expression - Options: ``"PASS"``/``"WARN"``/``"SKIP"``/``"FAIL"``, Default: ``"PASS""``
    :type on_null: str
    :param is_primary_key:  Default: ``False``
    :type is_primary_key: Default: ``bool``
    :param is_event_time: Default: ``False``
    :type is_event_time: bool
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

        self.sql = sqlparse.format(
            _interpreters["beeswax_duckdb"].to_sql(self), reindent=True
        )
        self._node_data = {
            "label": rf"Expression ID: {id(self)}",
            "type": str(type(self)),
            "base": str(type(self).__bases__[0]),
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


class ColumnList(OrderedDict):
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

    def __init__(self, columns: List[Any]) -> None:
        super().__init__([(column.name, column) for column in columns])

    def __getitem__(self, item):
        if isinstance(item, List):
            return [dict.__getitem__(self, key) for key in item]

        else:
            return dict.__getitem__(self, item)

    def list_columns(self, filter_regex: str = "", filter_unit: Unit = None):
        if filter_unit:
            if filter_regex:
                return [
                    column
                    for column in list(self.values())
                    if re.match(filter_regex, column.name)
                    and column.unit == filter_unit
                ]
            else:
                return [
                    column
                    for column in list(self.values())
                    if column.unit == filter_unit
                ]

        else:
            if filter_regex:
                return [
                    column
                    for column in list(self.values())
                    if re.match(filter_regex, column.name)
                ]
            else:
                return [column for column in list(self.values())]

    def list_names(self, filter_regex: str = "", filter_unit: Unit = None):
        if filter_unit:
            if filter_regex:
                return [
                    column.name
                    for column in list(self.values())
                    if re.match(filter_regex, column.name)
                    and column.unit == filter_unit
                ]
            else:
                return [
                    column.name
                    for column in list(self.values())
                    if column.unit == filter_unit
                ]

        else:
            if filter_regex:
                return [
                    column.name
                    for column in list(self.values())
                    if re.match(filter_regex, column.name)
                ]
            else:
                return list(self.keys())

    def add(self, column):
        self[column.name] = column

    def __add__(self, other):
        if list(set(self.list_names()).intersection(set(other.list_names()))):
            raise ValueError(
                rf"""
            Column name overlap: {list(set(self.list_names()).intersection(set(other.list_names())))}
            """
            )

        return ColumnList(self.list_columns() + other.list_columns())


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
        super().__init__(columns=columns.list_columns())
        self.sql = sqlparse.format(
            _interpreters["beeswax_duckdb"].to_sql(self), reindent=True
        )


class _Value:
    """
    Value lineage object

    :param value: Value
    :param data_type: lineage.values.Datatype - Data type of value
    :param var_type: str - "categorical"/"numeric"/"key"
    :param unit: lineage.Unit = lineage.Unit() - Unit of value
    """

    def __init__(
        self,
        value,
        data_type,
        var_type: str = None,
        unit: Unit = Unit(),
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
            "sql": self.sql,
        }

        graph = rx.PyDiGraph()
        graph.add_node(self._node_data)
        self.graph = LineageGraph(rx_graph=graph)

    def root_graph(self):
        return self.graph

    def __eq__(self, other):
        if isinstance(other, _Value):
            if (
                self.data_type == other.data_type
                and self.value == other.value
                and self.unit == other.unit
            ):
                return True
            else:
                return False
        else:
            return False


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
        self.sql = sqlparse.format(
            _interpreters["beeswax_duckdb"].to_sql(self), reindent=True
        )

        self._node_data = {
            "label": rf"Condition ID: {id(self)}",
            "type": str(type(self)),
            "base": str(type(self).__bases__[0]),
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


class _Function:
    """
    Function lineage object

    :param name: str - Function name
    :param args: List[Any] - Function arguments in order
    :param data_type: lineage.values.Datatype - Data type of output
    :param var_type: str - "categorical"/"numeric"/"key"
    :param partition_by: lineage.core.PartitionBy = lineage.core.PartitionBy(ColumnList([]))
    :param order_by: lineage.core.OrderBy = lineage.core.OrderBy(columns=ColumnList([]), how=[])
    :param unit: lineage.Unit = lineage.Unit() - Unit of output
    """

    def __init__(
        self,
        name: str,
        args: List[Any],
        data_type=None,
        var_type: str = None,
        partition_by: PartitionBy = PartitionBy(ColumnList([])),
        order_by: OrderBy = OrderBy(columns=ColumnList([]), how=[]),
        unit: Unit = Unit(),
        distinct: bool = False,
        framing: _Expression = None,
        filter: Condition = None,
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
        self.filter = filter

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


class CaseWhen:
    def __init__(
        self,
        conditions: List[Condition],
        values: List[Any],
        else_value=None,
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
        unit: Unit = Unit(),
        on_null: str = "PASS",
        is_primary_key: bool = False,
        is_event_time: bool = False,
    ):
        self.name = name
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
            "sql": self.sql,
        }


class Record:
    def __init__(self, values: dict):
        self.values = list(values.values())
        self.columns = ColumnList(list(values.keys()))
        self.sql = sqlparse.format(
            _interpreters["beeswax_duckdb"].to_sql(self), reindent=True
        )

        self._node_data = {
            "type": str(type(self)),
            "sql": self.sql,
        }

        graph = rx.PyDiGraph()
        graph.add_node(self._node_data)
        self.graph = LineageGraph(rx_graph=graph)


class RecordList:
    def __init__(self, name: str, records: List[Record]):
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
        self.sql = sqlparse.format(
            _interpreters["beeswax_duckdb"].to_sql(self), reindent=True
        )

        self._node_data = {
            "type": str(type(self)),
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
    ):
        if not isinstance(generator([1], generator_args).__next__(), Record):
            raise ValueError("generator must return Record object")

        self.name = name
        self.generator = generator([x for x in range(n_records)], generator_args)
        self.columns = generator([1], generator_args).__next__().columns

        for column in self.columns.list_columns():
            setattr(column, "source_table", None)
            setattr(column, "current_table", None)
        self.sql = sqlparse.format(
            _interpreters["beeswax_duckdb"].to_sql(self), reindent=True
        )
        self._node_data = {
            "type": str(type(self)),
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
        self.order_by = order_by

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
            "sql": self.sql,
        }

        graph = rx.PyDiGraph()
        graph.add_node(self._node_data)
        graph = LineageGraph(graph)

        if isinstance(self.source, TableList):
            for table in self.source.list_tables():
                graph.add_parent(0, table._node_data, {})
        else:
            graph.add_parent(0, source._node_data, {})

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

        for column in self.columns.list_columns():
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
        for column in columns.list_columns():
            self.add_column(column)
            setattr(column, "sql", _interpreters["beeswax_duckdb"].to_sql(column))
        self.sql = sqlparse.format(
            _interpreters["beeswax_duckdb"].to_sql(self), reindent=True
        )

        self.update_graph()

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
        self.sql = sqlparse.format(
            _interpreters["beeswax_duckdb"].to_sql(self), reindent=True
        )

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
        self.sql = sqlparse.format(
            _interpreters["beeswax_duckdb"].to_sql(self), reindent=True
        )

        self.update_graph()

    def root_graph(self):
        graph = self.graph

        graph.union([column.root_graph() for column in self.columns.list_columns()])

        if type(self.source) is ColumnList:
            graph.union([table.root_graph() for table in self.source.list_tables()])
        else:
            graph.union([self.source.root_graph()])

        return graph


class TableList(OrderedDict):
    def __init__(self, tables: List[Any]) -> None:
        super().__init__([(table.name, table) for table in tables])

    def list_tables(self):
        return list(self.values())

    def list_names(self):
        return list(self.keys())

    def __getitem__(self, item):
        if isinstance(item, List):
            return [dict.__getitem__(self, key) for key in item]

        else:
            return dict.__getitem__(self, item)

    def add(self, table, override: bool = False):
        if not isinstance(table, _Table):
            raise ValueError("table must be _Table object")

        if table.name in self.list_names() and not override:
            raise ValueError(rf"_Table '{table.name}' exists in TableList")

        self[table.name] = table


class _Transformation:
    def __init__(self, name, source, args):
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

    def __init__(self, source, operator):
        self.source = source
        self.operator = operator
        self.sql = sqlparse.format(
            _interpreters["beeswax_duckdb"].to_sql(self), reindent=True
        )

        self._node_data = {
            "label": rf"AppendOperator - {id(self)}",
            "type": str(type(self)),
            "base": str(type(self).__bases__[0]),
            "sql": self.sql,
        }
        graph = rx.PyDiGraph()
        graph.add_node(self._node_data)

        graph.add_child(0, self.source._node_data, {})

        self.graph = LineageGraph(rx_graph=graph)


class PrependOperator:

    """
    **PrependOperator** behaves similarly to an **Expression** object with only the left side.

    :param source: Object to apply operator to
    :type source: Any
    :param operator: Operator to apply
    :type operator: lineage._Operator
    """

    def __init__(self, source, operator):
        self.source = source
        self.operator = operator
        self.sql = sqlparse.format(
            _interpreters["beeswax_duckdb"].to_sql(self), reindent=True
        )

        self._node_data = {
            "label": rf"PrependOperator - {id(self)}",
            "type": str(type(self)),
            "base": str(type(self).__bases__[0]),
            "sql": self.sql,
        }
        graph = rx.PyDiGraph()
        graph.add_node(self._node_data)

        graph.add_child(0, self.source._node_data, {})

        self.graph = LineageGraph(rx_graph=graph)
