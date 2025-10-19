import pandas as pd
from ..values import Varchar, Datatype, Boolean
from ..columns import WildCard
from ..core import ColumnList, TableList, _Table, LineageGraph, _Transformation
from .. import tables
from ...interpreter import Interpreter
from .core import _Schema, _SchemaSettings
from units.core import Unit
import json
from typing import List, Dict
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

    return {
        dataset: {
            column.name: ColumnMetadata(column)
            for index, column in column_metadata[
                column_metadata["dataset"] == dataset
            ].iterrows()
        }
        for dataset in column_metadata.dataset.unique()
    }


def read_file_metadata(filepath: str, separator: str = "\t"):
    file_metadata = pd.read_csv(filepath, sep=separator)

    file_metadata["distinct"] = file_metadata["distinct"].astype(bool)

    for column in file_metadata.columns.tolist():
        file_metadata[column] = file_metadata[column].fillna("")
        file_metadata[column] = file_metadata[column].astype(str)

    return {
        file.dataset: FileMetadata(file) for index, file in file_metadata.iterrows()
    }


class ColumnMetadata:

    """
    The ColumnMetadata object takes the following pd.Series as an argument

    :param schema: Schema containing table
    :type schema: str
    :param dataset: Dataset name. Will be used as table name in schema
    :type dataset: str
    :param column_name: Column name in source.
    :type column_name: str
    :param column_alias: Column alias.
    :type column_alias: str
    :param var_type: Options: Variable type. Used to determine which validation tests are run.
     ``'numeric'``/``'categorical'``/``'string'``/``'timedelta'``/``'datetime'``/``'key'``/``'sequential'``/``''``
     Default: ``''``
    :type var_type: str
    :param data_type: SQL data_type of column to cast to
    :type data_type: str
    :param source_unit: Unit of measurement of column in source file. Default: ``''``
    :type source_unit: str
    :param target_unit: Target unit of measurement to convert to in source table. Default: ``''``
    :type target_unit: str
    :param precision: Precision to round column to. Options: ``'{n}dp'``/``'{n}sf'`` Default: ``''``
    :type precision: str
    :param scale_factor: Value to multiply column by to scale to source_unit. Default: ``''``
    :type scale_factor: float
    :param filter_values: Values to filter. Default: ``''``
    :type filter_values: List[Any]
    :param on_filter: Behaviour on filter value. Options: ``'PASS'``/``'FAIL'``/``'WARN'``/``'SKIP'`` Default: ``'PASS'``
    :type on_filter: str
    :param on_null: Behaviour on NULL value. Options: ``'PASS'``/``'FAIL'``/``'WARN'``/``'SKIP'`` Default: ``'PASS'``
    :type on_null: str
    :param is_primary_key: Default: ``False``
    :type is_primary_key: bool
    :param is_event_time: Default: ``False``
    :type is_event_time: bool
    :param regex: Default: ``''``
    :type regex: str
    :param ordinal_position: Column order
    :type ordinal_position: int
    """

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
        self._node_data = {
            "dataset": self.dataset,
            "column_name": self.column_name,
            "column_alias": self.column_alias,
            "var_type": self.var_type,
            "data_type": self.data_type.value,
            "source_unit": self.source_unit.name,
            "target_unit": self.target_unit.name,
            "precision": self.precision,
            "scale_factor": str(self.scale_factor),
            "filter_values": str(self.filter_values),
            "on_filter": self.on_filter,
            "on_null": self.on_null,
            "is_primary_key": str(self.is_primary_key),
            "is_event_time": str(self.is_event_time),
            "regex": self.regex,
            "ordinal_position": str(self.ordinal_position),
            "schema": self.schema,
            "type": str(type(self)),
            "base": str(type(self)),
            "label": self.dataset + "." + self.column_name,
        }

        self._edge_data = {
            "is_primary_key": str(self.is_primary_key),
            "is_event_time": str(self.is_event_time),
        }

        graph = rx.PyDiGraph()
        graph.add_node(self._node_data)

        self.graph = LineageGraph(rx_graph=graph)

    def root_graph(self):
        return self.graph

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
        self._node_data = {
            "dataset": self.dataset,
            "file_regex": self.file_regex,
            "delim": self.delim,
            "distinct": str(self.distinct),
            "schema": self.schema,
            "type": str(type(self)),
            "base": str(type(self)),
            "label": rf"{self.dataset} - {self.file_regex}",
        }

        graph = rx.PyDiGraph()
        graph.add_node(self._node_data)

        self.graph = LineageGraph(rx_graph=graph)

    def __getitem__(self, item):
        if isinstance(item, list):
            return [getattr(self, value) for value in item]
        else:
            return getattr(self, item)


class SourceFile:
    def __init__(
        self,
        file_metadata: FileMetadata,
        expected_column_metadata: Dict[str, ColumnMetadata],
    ) -> None:
        self.name = file_metadata.dataset
        self.dataset = Varchar(file_metadata.dataset)
        self.file_regex = Varchar(file_metadata.file_regex)
        self.delim = Varchar(file_metadata.delim)
        self.distinct = file_metadata.distinct
        self.expected_column_metadata = expected_column_metadata
        self.sql = _interpreters["beeswax_duckdb"].to_sql(self)
        self._node_data = file_metadata._node_data
        self.graph = file_metadata.graph
        self.extension = Varchar(self.file_regex.value.split(".")[-1])

        for column in self.expected_column_metadata.values():
            self.graph.add_child(0, column._node_data, column._edge_data)


class ReadCSV(_Transformation):
    def __init__(
        self,
        source_file: SourceFile,
        union_by_name: Boolean = Boolean(False),
        headers: Boolean = Boolean(False),
        macro_group: str = "",
        all_varchar: Boolean = Boolean(False),
    ):
        super().__init__(
            name="READ_CSV",
            source=source_file,
            args={
                "union_by_name": union_by_name,
                "header": headers,
                "all_varchar": all_varchar,
            },
            macro_group=macro_group,
        )


class ReadGeoJson(_Transformation):
    def __init__(
        self,
        source_file: SourceFile,
        macro_group: str = "",
    ):
        if source_file.extension.value in ["json", "geojson"]:
            super().__init__(
                name="ST_READ", source=source_file, args={}, macro_group=macro_group
            )
        else:
            raise ValueError(
                rf"SourceFile extension not compatible with ReadGeoJson: {source_file.extension.value}"
            )


class SourceSettings(_SchemaSettings):
    def __init__(
        self,
        file_metadata: Dict[str, FileMetadata],
        expected_column_metadata: Dict[str, Dict[str, ColumnMetadata]],
        substitutions={},
        extensions: List[Dict[str, str]] = [],
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

        for file in settings.file_metadata.values():
            source_file = SourceFile(
                file_metadata=file,
                expected_column_metadata=settings.expected_column_metadata[
                    file.dataset
                ],
            )

            if source_file.extension.value in ["json", "geojson"]:
                source_table = tables.Core(
                    name=source_file.dataset.value,
                    columns=ColumnList([WildCard()]),
                    source=ReadGeoJson(source_file),
                    distinct=source_file.distinct,
                )
            else:
                source_table = tables.Core(
                    name=source_file.dataset.value,
                    columns=ColumnList([WildCard()]),
                    source=ReadCSV(
                        source_file,
                        union_by_name=Boolean(True),
                        headers=Boolean(True),
                        all_varchar=Boolean(True),
                    ),
                    distinct=source_file.distinct,
                )

            setattr(
                source_table,
                "expected_column_metadata",
                source_file.expected_column_metadata,
            )

            source_tables.add_(source_table)

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
