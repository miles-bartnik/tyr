from .core import TableList
import pickle
import os
import pandas as pd
import yaml
from typing import Dict, List


class SchemaSettings:
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


class Schema:
    """
    Storage object for schema settings and tables.

    Args:
        - settings:lineage.schema.SchemaSettings - Schema settings
        - tables:lineage.core.TableList - List of tables within the schema
    """

    def __init__(self, settings: SchemaSettings, tables=TableList([])) -> None:
        self.settings = settings
        self.tables = tables

        for table in self.tables.list_all():
            setattr(table, "schema", self.settings.name)

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

    def drop_tables(self, tables: List[str]):
        for table in tables:
            delattr(self.tables, table)

    def keep_tables(self, tables: List[str]):
        for table in self.tables.list_names():
            if table not in tables:
                delattr(self.tables, table)
