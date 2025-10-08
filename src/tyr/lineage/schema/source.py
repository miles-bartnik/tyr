import pandas as pd
from ..values import Varchar, Datatype
from ..columns import WildCard
from ..core import ColumnList, TableList, _Table
from .. import tables
from ...interpreter import Interpreter
from .core import _Schema, _SchemaSettings
from units.core import Unit
import json
from typing import List
import os
import re
import rustworkx as rx

_interpreters = {"beeswax_duckdb": Interpreter()}


def read_column_metadata(filepath: str, separator: str = "\t"):
    column_metadata = pd.read_csv(filepath, sep=separator)

    column_metadata["ordinal_position"] = column_metadata["ordinal_position"].astype(
        int
    )

    column_metadata["is_primary_key"] = column_metadata["is_primary_key"].astype(bool)
    column_metadata["is_event_time"] = column_metadata["is_event_time"].astype(bool)
    column_metadata["filter_values"] = column_metadata["filter_values"].fillna("[]")

    for column in [
        column
        for column in column_metadata.columns.tolist()
        if column
        not in ["ordinal_position", "is_primary_key", "is_event_time", "filter_values"]
    ]:
        column_metadata[column] = column_metadata[column].fillna("")
        column_metadata[column] = column_metadata[column].astype(str)

    return column_metadata


def read_file_metadata(filepath: str, separator: str = "\t"):
    file_metadata = pd.read_csv(filepath, sep=separator)

    file_metadata["distinct"] = file_metadata["distinct"].astype(bool)

    for column in file_metadata.columns.tolist():
        file_metadata[column] = file_metadata[column].fillna("")
        file_metadata[column] = file_metadata[column].astype(str)

    return file_metadata


class ColumnMetadata:
    def __init__(self, column_metadata: pd.Series):
        self.dataset = str(column_metadata["dataset"])
        self.column_name = str(column_metadata["column_name"])
        self.column_alias = str(column_metadata["column_alias"])
        self.var_type = str(column_metadata["var_type"])
        self.data_type = Datatype(str(column_metadata["data_type"]))
        self.source_unit = Unit(str(column_metadata["source_unit"]))
        if column_metadata["target_unit"]:
            self.target_unit = Unit(str(column_metadata["target_unit"]))
        else:
            self.target_unit = Unit(str(column_metadata["source_unit"]))
        self.precision = str(column_metadata["precision"])
        if column_metadata["scale_factor"]:
            self.scale_factor = float(column_metadata["scale_factor"])
        elif self.var_type == "numeric":
            self.scale_factor = 1
        else:
            self.scale_factor = None
        self.filter_values = json.loads(column_metadata["filter_values"])
        self.on_filter = str(column_metadata["on_filter"])
        self.on_null = str(column_metadata["on_null"])
        self.is_primary_key = bool(column_metadata["is_primary_key"])
        self.is_event_time = bool(column_metadata["is_event_time"])
        self.regex = str(column_metadata["regex"])
        self.ordinal_position = int(column_metadata["ordinal_position"])
        self.schema = str(column_metadata["schema"])

    def __getitem__(self, item):
        if isinstance(item, list):
            return [getattr(self, value) for value in item]
        else:
            return getattr(self, item)


class FileMetadata:
    def __init__(self, file_metadata: pd.Series):
        self.dataset = str(file_metadata["dataset"])
        self.file_regex = str(file_metadata["file_regex"])
        self.delim = str(file_metadata["delim"])
        self.distinct = bool(file_metadata["distinct"])
        self.schema = str(file_metadata["schema"])

    def __getitem__(self, item):
        if isinstance(item, list):
            return [getattr(self, value) for value in item]
        else:
            return getattr(self, item)


class SourceFile:
    def __init__(
        self,
        file_metadata: FileMetadata,
        expected_column_metadata: List[ColumnMetadata],
    ) -> None:
        self.name = file_metadata.dataset
        self.dataset = Varchar(file_metadata.dataset)
        self.file_regex = Varchar(file_metadata.file_regex)
        self.delim = Varchar(file_metadata.delim)
        self.distinct = file_metadata.distinct
        self.expected_column_metadata = expected_column_metadata
        self.sql = _interpreters["beeswax_duckdb"].to_sql(self)

        self._node_data = {
            "label": self.file_regex.value,
            "dataset": self.dataset.value,
            "delim": self.delim.value,
            "type": str(type(self)),
            "base": str(type(self).__bases__[0]),
            "sql": self.sql,
        }

        graph = rx.PyDiGraph()
        graph.add_node(self._node_data)
        self.graph = graph


class SourceSettings(_SchemaSettings):
    def __init__(
        self,
        file_metadata: List[FileMetadata],
        expected_column_metadata: List[ColumnMetadata],
        substitutions={},
        extensions={},
        connection={},
    ):
        super().__init__(
            name="source",
            substitutions=substitutions,
            extensions=extensions,
            connection=connection,
        )

        self.file_metadata = file_metadata
        self.expected_column_metadata = expected_column_metadata


class Source(_Schema):
    def __init__(self, settings: SourceSettings):
        source_tables = TableList([])

        expected_column_metadata = settings.expected_column_metadata

        for file in settings.file_metadata:
            source_file = SourceFile(
                file_metadata=file,
                expected_column_metadata=[
                    column
                    for column in settings.expected_column_metadata
                    if column.dataset == file.dataset
                ],
            )

            source_table = tables.Core(
                name=source_file.dataset.value,
                columns=ColumnList([WildCard()]),
                source=source_file,
                distinct=source_file.distinct,
            )

            setattr(
                source_table,
                "expected_column_metadata",
                source_file.expected_column_metadata,
            )

            source_tables.add(source_table)

        super().__init__(settings=settings, tables=source_tables)

        return


def init_column_metadata(
    path: str = None,
    file_metadata: pd.DataFrame = pd.DataFrame(),
):
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
            if not [
                "/".join(row["file_regex"].split("/")[:-1]) + "/" + file
                for file in os.listdir("/".join(row["file_regex"].split("/")[:-1]))
                if re.search(
                    row["file_regex"].replace(".", "\.").replace("*", ".*"),
                    "/".join(row["file_regex"].split("/")[:-1]) + "/" + file,
                )
            ]:
                print(rf"""Nothing found for - {row['file_regex']}""")
                return column_metadata_df

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

    if not path:
        return column_metadata_df

    else:
        column_metadata_df.to_csv(path, sep="\t", header=True, index=False)


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
