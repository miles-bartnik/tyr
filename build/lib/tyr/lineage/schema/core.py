from ..core import TableList, _interpreters, LineageGraph
from ..tables import Core
import pickle
import os
import pandas as pd
import rustworkx as rx
from typing import Dict, List
import sqlparse


def load_schema_from_pkl(filepath):
    with open(filepath, "rb") as f:
        return pickle.load(f)


class _SchemaSettings:
    """
    Base class for storing schema settings. See lineage.macros.schema for example usage.

    :param name: str - Schema name
    :param substitutions: Dict - String substitutions to make in sql strings e.g. {'%run_id%':'abcdef_123456'}
    :param extensions: List[str] - List of extensions to install e.g. ['spatial']
    :param connection: Dict - Connection settings e.g. {"enable_progress_bar": True, "threads": 4}
    :param configuration: Dict - Any additional settings used by the tables within the datamodel e.g. {"min_datetime": '2025-01-01'}
    """

    def __init__(
        self,
        name: str,
        substitutions: Dict = {},
        extensions: List[Dict[str, str]] = {},
        connection: Dict = {"enable_progress_bar": True, "threads": 4},
        configuration: Dict = {},
    ) -> None:
        self.name = name
        self.substitutions = substitutions
        self.extensions = extensions
        self.connection = connection
        self.configuration = configuration
        self.sql = sqlparse.format(
            _interpreters["beeswax_duckdb"].to_sql(self), reindent=True
        )


class _Schema:
    """
    Storage object for schema settings and tables.

    Args:
        - settings:lineage.schema.SchemaSettings - Schema settings
        - tables:lineage.core.TableList - List of tables within the schema
    """

    def __init__(self, settings: _SchemaSettings, tables=TableList([])) -> None:
        self.name = settings.name
        self.settings = settings
        self.tables = tables
        self.sql = sqlparse.format(
            _interpreters["beeswax_duckdb"].to_sql(self), reindent=True
        )
        self._node_data = {
            "label": self.name,
            "name": self.name,
            "sql": self.sql,
            "type": str(type(self)),
            "base": str(type(self).__bases__[0]),
        }

        graph = rx.PyDiGraph()
        graph.add_node(self._node_data)
        graph = LineageGraph(graph)

        for table in self.tables.list_tables_():
            setattr(table, "schema", self)
            setattr(
                table, "_node_data", table._node_data | {"schema": self.settings.name}
            )
            table.update_graph()
            graph.add_child(0, table._node_data, {})

        self.graph = graph

        self._outbound_edge_data = {}
        self._inbound_edge_data = {}

    def save(self, output_directory: str = os.getcwd()):
        """
        Save schema to pickle object

        :param output_directory:str = os.path.expanduser(rf"~/")
        :return:
        """

        with open(
            rf"{output_directory.rstrip('/')}/{self.settings.name}.pkl", "wb"
        ) as f:
            pickle.dump(self, f, pickle.HIGHEST_PROTOCOL)

    def add_table(self, table: Core, override: bool = False):
        setattr(table, "schema", self)
        table.update_graph()

        try:
            self.tables.add_(table, override=override)
            self.graph.add_child(0, table._node_data, {})
        except:
            setattr(table, "schema", None)
            table.update_graph()
            self.tables.add_(table, override=override)
            self.graph.add_child(0, table._node_data, {})

    def add_tables(self, tables: TableList, override: bool = False):
        if not tables.is_empty:
            for table in tables.list_tables_():
                setattr(table, "schema", self)
                table.update_graph()
                try:
                    self.tables.add_(table, override=override)
                    self.graph.add_child(0, table._node_data, {})
                except:
                    setattr(table, "schema", None)
                    table.update_graph()
                    self.tables.add_(table, override=override)
                    self.graph.add_child(0, table._node_data, {})

    def drop_tables(self, tables: List[str] = [], force=False):
        if not tables:
            tables = self.tables.list_names_()

        if not force:
            joinstr = "\n - "

            prompt = rf"""
            Confirm tables to be dropped (y/[n]):{joinstr.join(tables)}
            """

            if input(prompt) == "y":
                for table in tables:
                    delattr(self.tables, table)

        else:
            for table in tables:
                delattr(self.tables, table)

    def root_graph(self):
        graph = self.graph

        graph.union([table.graph for table in self.tables.list_tables_()])

        return graph
