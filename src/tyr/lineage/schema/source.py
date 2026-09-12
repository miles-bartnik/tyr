import pandas as pd
from ..values import Varchar, Datatype, Boolean, Raw, Null, List
from ..columns import WildCard
from ..core import ColumnList, TableList, _Table, LineageGraph, _Transformation
from .. import tables
from ...interpreter import Interpreter
from .core import _Schema, _SchemaSettings
from ..units.core import Unit
import json
import typing
from pathlib import Path
import re
import rustworkx as rx
import ast

_interpreters = {"beeswax_duckdb": Interpreter()}


def _truthy(value):
    """Parse a metadata bool cell. TSV columns arrive as strings, so `bool('False')`
    would be True -- match the literal truthy tokens instead. NaN/blank -> False."""
    return str(value).strip().lower() in ("true", "1", "yes", "t")


def _resolve_file_regex(file_regex: str, base_dir=None) -> str:
    """Normalise a file_metadata path so it works on Windows or Linux, independent of
    the process working directory. Relative paths are resolved against ``base_dir``
    (the directory of the file_metadata file that declared them -- so paths are written
    relative to that file, e.g. ``../datasets/foo_*.tsv``); absolute paths pass through.
    ``base_dir`` defaults to the cwd when not supplied. Always returned as a POSIX string
    (forward slashes) -- DuckDB's readers accept those on every platform. The trailing
    glob (``*``) is preserved."""
    base = Path(base_dir) if base_dir else Path.cwd()
    return (base / file_regex).resolve().as_posix()


def read_column_metadata(filepath: str, separator: str = "\t"):
    column_metadata = pd.read_csv(filepath, sep=separator)

    column_metadata["ordinal_position"] = column_metadata["ordinal_position"].astype(
        int
    )

    # Linked-column fields may be absent in metadata files written before linking
    # existed -- default them to empty (no linking) rather than failing the read.
    for link_field in ["link_column", "link_mapping", "link_behaviour"]:
        if link_field not in column_metadata.columns:
            column_metadata[link_field] = ""

    # NB: is_primary_key / is_event_time are left as their raw string form here and
    # parsed with _truthy in ColumnMetadata -- astype(bool) on 'False' would be True.
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
            column["column_name"]: ColumnMetadata(column)
            for index, column in column_metadata[
                column_metadata["dataset"] == dataset
            ].iterrows()
        }
        for dataset in column_metadata.dataset.unique()
    }


def read_file_metadata(filepath: str, separator: str = "\t"):
    file_metadata = pd.read_csv(filepath, sep=separator)

    # `distinct` stays a string here and is parsed with _truthy in FileMetadata --
    # astype(bool) on the string 'False' would be True (a forced SELECT DISTINCT).
    for column in file_metadata.columns.tolist():
        file_metadata[column] = file_metadata[column].fillna("")
        file_metadata[column] = file_metadata[column].astype(str)

    # file_regex paths are resolved relative to this file's directory, so the metadata
    # is portable regardless of where the process is run from.
    base_dir = Path(filepath).parent
    return {
        file.dataset: FileMetadata(file, base_dir=base_dir)
        for index, file in file_metadata.iterrows()
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
    :param link_column: Name of another column in the same dataset to link this column to. Empty = no linking. Default: ``''``
    :type link_column: str
    :param link_mapping: Mapping for linked columns: ``'{"A":"value_1", "B":"value_2"} | "null_value"'``. The ``| "null_value"`` fallback segment is optional. Default: ``''``
    :type link_mapping: str
    :param link_behaviour: When linking fires. Options: ``'ON_NULL'``/``'OVERWRITE'``. Required when ``link_column`` is set. Default: ``''``
    :type link_behaviour: str
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

        if json.loads(column_metadata["filter_values"]):
            self.filter_values = json.loads(column_metadata["filter_values"])
        else:
            self.filter_values = []
        self.on_filter = str(column_metadata["on_filter"])
        self.on_null = str(column_metadata["on_null"])
        self.link_column = str(column_metadata["link_column"])
        self.link_mapping = str(column_metadata["link_mapping"])
        self.link_behaviour = str(column_metadata["link_behaviour"])

        if column_metadata["default_value"]:
            if "[]" in self.data_type.value:
                self.default_value = self.data_type.to_value(
                    value=[
                        Datatype(self.data_type.value.replace("[]", "")).to_value(value)
                        for value in ast.literal_eval(column_metadata.default_value)
                    ],
                    unit=self.source_unit,
                )
            else:
                self.default_value = self.data_type.to_value(
                    value=column_metadata.default_value,
                    unit=self.source_unit,
                )

        else:
            self.default_value = Null(data_type=self.data_type)

        self.is_primary_key = _truthy(column_metadata["is_primary_key"])
        self.is_event_time = _truthy(column_metadata["is_event_time"])
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
            "link_column": self.link_column,
            "link_mapping": self.link_mapping,
            "link_behaviour": self.link_behaviour,
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
    def __init__(self, file_metadata: pd.Series, base_dir=None):
        self.dataset = str(file_metadata["dataset"])
        self.file_regex = _resolve_file_regex(
            str(file_metadata["file_regex"]), base_dir
        )
        self.delim = str(file_metadata["delim"])
        self.distinct = _truthy(file_metadata["distinct"])
        self.schema = str(file_metadata["schema"])
        # 'required' boolean flag (file_metadata TSV column). Absent column
        # (older TSVs) and empty values mean required -- the file MUST exist
        # for the schema to be complete. Only an explicit false/0/no marks an
        # optional dataset: the consumer skips a missing file instead of
        # failing. Mirrors apian tyrbuild's required parsing exactly.
        self.required = (str(file_metadata["required"]).strip().lower()
                         not in ("false", "0", "no")
                         if "required" in file_metadata else True)
        self._node_data = {
            "dataset": self.dataset,
            "file_regex": self.file_regex,
            "delim": self.delim,
            "distinct": str(self.distinct),
            "schema": self.schema,
            "required": str(self.required),
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
        expected_column_metadata: typing.Dict[str, ColumnMetadata],
    ) -> None:
        if file_metadata.delim == "t":
            delim = r"'\t'"
        elif file_metadata.delim == "c":
            delim = r"','"
        else:
            delim = rf"'{file_metadata.delim}'"

        self.name = file_metadata.dataset
        self.dataset = Varchar(file_metadata.dataset)
        self.file_regex = Varchar(file_metadata.file_regex)
        self.delim = Raw(delim)
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
        all_varchar: Boolean = Boolean(False),
    ):
        super().__init__(
            name="READ_CSV",
            source=source_file,
            args={
                "delim": source_file.delim,
                "union_by_name": union_by_name,
                "header": headers,
                "all_varchar": all_varchar,
            },
        )


class ReadGeoJson(_Transformation):
    def __init__(
        self,
        source_file: SourceFile,
    ):
        if source_file.extension.value in ["json", "geojson"]:
            if "*" in source_file.file_regex.value:
                super().__init__(name="ST_READ_MULTI", source=source_file, args={})
            else:
                super().__init__(name="ST_READ", source=source_file, args={})
        else:
            raise ValueError(
                rf"SourceFile extension not compatible with ReadGeoJson: {source_file.extension.value}"
            )


class SourceSettings(_SchemaSettings):
    def __init__(
        self,
        file_metadata: typing.Dict[str, FileMetadata],
        expected_column_metadata: typing.Dict[str, typing.Dict[str, ColumnMetadata]],
        substitutions={},
        extensions: typing.List[typing.Dict[str, str]] = [],
        connection=None,
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
                    required=file.required,
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
                    required=file.required,
                )

            setattr(
                source_table,
                "expected_column_metadata",
                source_file.expected_column_metadata,
            )

            source_tables.add(source_table)

        super().__init__(settings=settings, tables=source_tables)

        return


def _var_type_for(data_type: str) -> str:
    """Map a DuckDB / SQL data-type string to a tyr var_type."""
    d = str(data_type).upper()
    if any(k in d for k in ("INT", "DOUBLE", "DECIMAL", "FLOAT", "REAL", "BIGINT", "NUMERIC")):
        return "numeric"
    if "TIMESTAMP" in d or "DATE" in d or "TIME" in d:
        return "timestamp"
    return "categorical"


def _match_files(resolved_path: str):
    """Return all files matching a resolved path that may contain shell-style
    glob wildcards (*, ?). Matches are against filenames only, so a pattern like
    ``dir/foo*`` only matches ``foo*`` files inside ``dir`` and never a parent
    directory named ``foodir``."""
    directory = Path(resolved_path).parent
    filename_pattern = Path(resolved_path).name
    # Escape regex metacharacters, then restore * and ? as wildcards.
    escaped = re.escape(filename_pattern).replace(r"\*", ".*").replace(r"\?", ".")
    regex = re.compile(f"^{escaped}$")
    return [p.as_posix() for p in directory.iterdir() if regex.match(p.name)]


def _detect_columns(file_pattern: str, delim: str, ext: str):
    """Infer (name, data_type) columns for a source file pattern using DuckDB.

    CSV/TSV files use ``read_csv_auto``; GeoJSON/JSON use ``ST_Read``. The
    spatial extension is loaded on demand. If DuckDB cannot read a GeoJSON file,
    the property keys are returned with types inferred from their JSON values.
    """
    import duckdb

    con = duckdb.connect()
    try:
        if ext in ("json", "geojson"):
            con.execute("INSTALL spatial; LOAD spatial;")
            safe = str(file_pattern).replace("'", "''")
            return con.execute(
                f"DESCRIBE SELECT * FROM ST_Read('{safe}')"
            ).fetchall()

        reader = f"read_csv_auto('{str(file_pattern).replace(chr(39), chr(39)+chr(39))}'"
        if delim:
            reader += f", delim='{delim}'"
        reader += ")"
        return con.execute(f"DESCRIBE SELECT * FROM {reader}").fetchall()
    except Exception:
        # Fallback for GeoJSON when the spatial extension is unavailable.
        if ext in ("json", "geojson"):
            matches = _match_files(file_pattern)
            if matches:
                return _geojson_property_columns(matches[0])
        raise
    finally:
        con.close()


def _geojson_property_columns(path: str):
    """Return (name, data_type) tuples for every property in the GeoJSON.

    Types are inferred from the decoded JSON values across ALL features (not
    just the first), mirroring DuckDB's inference: integral numbers -> INTEGER,
    any fractional number -> DOUBLE, all-boolean -> BOOLEAN, and VARCHAR when
    nothing better can be told (null-only, strings, or mixed kinds). A blanket
    VARCHAR here used to strand numeric properties (e.g. a street width_m) with
    a metadata type that disagreed with the data they actually carry.
    """
    with open(path, encoding="utf-8") as f:
        data = json.load(f)

    values_by_property = {}
    for feature in data.get("features", []):
        for key, value in (feature.get("properties") or {}).items():
            values_by_property.setdefault(key, []).append(value)

    return [
        (key, _json_values_data_type(values))
        for key, values in values_by_property.items()
    ]


def _json_values_data_type(values: list) -> str:
    """Single DuckDB-style data type for a list of decoded JSON values."""
    kinds = set()
    for value in values:
        if value is None:
            continue
        if isinstance(value, bool):
            kinds.add("BOOLEAN")
        elif isinstance(value, int):
            kinds.add("INTEGER")
        elif isinstance(value, float):
            kinds.add("DOUBLE")
        else:
            kinds.add("VARCHAR")

    if kinds == {"BOOLEAN"}:
        return "BOOLEAN"
    if kinds and kinds <= {"INTEGER", "DOUBLE"}:
        # any fractional value promotes the column, as read_csv/json auto do
        return "DOUBLE" if "DOUBLE" in kinds else "INTEGER"
    return "VARCHAR"


def init_column_metadata(
    path: str = None,
    file_metadata: pd.DataFrame = pd.DataFrame(),
    base_dir=None,
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
            "link_column",
            "link_mapping",
            "link_behaviour",
            "regex",
            "source_unit",
            "target_unit",
            "precision",
            "scale_factor",
            "default_value",
            "ordinal_position",
        ]
    )

    if file_metadata.empty:
        if not path:
            return column_metadata_df
        column_metadata_df.to_csv(path, sep="\t", header=True, index=False)
        return

    for index, row in file_metadata.iterrows():
        resolved = _resolve_file_regex(row["file_regex"], base_dir)
        matches = _match_files(resolved)

        if not matches:
            print(rf"""Nothing found for - {row['file_regex']}""")
            continue

        if row["delim"] == "t":
            delim = "\t"
        elif row["delim"] == "c":
            delim = ","
        else:
            delim = row["delim"]

        ext = str(row["file_regex"]).split(".")[-1].lower()

        try:
            info = _detect_columns(resolved, delim, ext)
        except Exception as e:
            print(rf"""Column detection failed for {row['file_regex']}: {e}""")
            continue

        rows = []
        for i, (name, dtype, *_rest) in enumerate(info):
            rows.append(
                {
                    "schema": row["schema"],
                    "dataset": row["dataset"],
                    "column_name": name,
                    "column_alias": None,
                    "var_type": _var_type_for(dtype),
                    "data_type": str(dtype),
                    "on_null": "PASS",
                    "is_primary_key": False,
                    "is_event_time": False,
                    "filter_values": None,
                    "on_filter": "PASS",
                    "link_column": None,
                    "link_mapping": None,
                    "link_behaviour": None,
                    "regex": None,
                    "source_unit": None,
                    "target_unit": None,
                    "precision": None,
                    "scale_factor": None,
                    "default_value": None,
                    "ordinal_position": i,
                }
            )

        if rows:
            column_metadata_df = pd.concat(
                [column_metadata_df, pd.DataFrame.from_records(rows)],
                ignore_index=True,
            )

    if not path:
        return column_metadata_df

    column_metadata_df.to_csv(path, sep="\t", header=True, index=False)


def init_file_metadata(
    path: str = None,
):
    if not path:
        configurations = Path.cwd() / "configurations"
        configurations.mkdir(exist_ok=True)
        path = configurations / "file_metadata.tsv"

    pd.DataFrame(
        columns=[
            "schema",
            "dataset",
            "file_regex",
            "delim",
        ]
    ).to_csv(path, sep="\t", header=True, index=False)
