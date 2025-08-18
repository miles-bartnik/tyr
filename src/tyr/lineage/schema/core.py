from ..core import TableList, _interpreters
import pickle
import os
import pandas as pd
import yaml
from typing import Dict, List


class _SchemaSettings:
    """
    Base class for storing schema settings. See lineage.macros.schema for example usage.

    :param name: str - Schema name
    :param substitutions: Dict - String substitutions to make in sql strings e.g. {'%run_id%':'abcdef_123456'}
    :param extensions: List[str] - List of extensions to install e.g. ['spatial']
    :param connection: Dict - Connection settings e.g. {"enable_progress_bar": True, "threads": 4}
    """

    def __init__(
        self,
        name: str,
        substitutions: Dict = {},
        extensions: List[str] = [],
        connection: Dict = {"enable_progress_bar": True, "threads": 4},
    ) -> None:
        self.name = name
        self.substitutions = substitutions
        self.extensions = extensions
        self.connection = connection
        self.sql = _interpreters["beeswax_duckdb"].to_sql(self)


class _Schema:
    """
    Storage object for schema settings and tables.

    Args:
        - settings:lineage.schema.SchemaSettings - Schema settings
        - tables:lineage.core.TableList - List of tables within the schema
    """

    def __init__(self, settings: _SchemaSettings, tables=TableList([])) -> None:
        self.settings = settings
        self.tables = tables
        self.sql = _interpreters["beeswax_duckdb"].to_sql(self)
        self._node_data = {
            "label": self.settings.name,
            "name": self.settings.name,
            "sql": self.sql,
            "type": str(type(self)),
            "base": str(type(self).__bases__[0]),
        }

        for table in self.tables.list_all():
            setattr(table, "schema", self)
            setattr(
                table, "_node_data", table._node_data | {"schema": self.settings.name}
            )

        self._outbound_edge_data = {}
        self._inbound_edge_data = {}

    def save(self, output_directory: str = os.path.expanduser(rf"~/")):
        """
        Save schema to pickle object

        :param output_directory:str = os.path.expanduser(rf"~/")
        :return:
        """

        with open(
            rf"{output_directory.rstrip('/')}/{self.settings.name}.pkl", "wb"
        ) as f:
            pickle.dump(self, f, pickle.HIGHEST_PROTOCOL)

    def drop_tables(self, tables: List[str] = [], force=False):
        if not tables:
            tables = self.tables.list_names()

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
