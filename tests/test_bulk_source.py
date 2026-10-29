"""Bulk-source ingestion: datasets whose rows are supplied programmatically
(pyarrow Table / pandas DataFrame / list of dicts) instead of read from a
source file.

The source schema's table for a bulk dataset renders as
``SELECT * FROM _tyr_bulk_<dataset>``; tyr.database.bulk registers the
caller's rows under that name before the build. Everything downstream --
staging column transforms, on_null DEFAULT policies, filters, links -- is
unchanged, and NULLs transported via pyarrow/nullable dtypes behave
exactly like file-read NULLs (the INTEGER coercion problem of numpy-only
transports does not exist here: a NULL stays NULL and the metadata
DEFAULT fills it, while a genuine 0 stays 0).

Helpers mirror tests/test_regex_mismatch_policy.py (self-contained copy so
the files stay independently runnable).
"""

import pandas as pd
import pytest
import tyr

from tyr.database.bulk import (
    placeholder_name,
    register_bulk_sources,
    unregister_bulk_sources,
)
from tyr.lineage.schema import source as source_schema
from tyr.lineage.schema import staging as staging_schema

COLUMN_HEADERS = [
    "schema",
    "dataset",
    "column_name",
    "column_alias",
    "var_type",
    "data_type",
    "on_null",
    "default_value",
    "is_primary_key",
    "is_event_time",
    "filter_values",
    "on_filter",
    "link_column",
    "link_mapping",
    "link_behaviour",
    "regex",
    "on_regex_mismatch",
    "source_unit",
    "target_unit",
    "scale_factor",
    "precision",
    "ordinal_position",
]


def metadata_row(**overrides):
    row = {
        "schema": "source",
        "dataset": "trees",
        "column_name": "species",
        "column_alias": "",
        "var_type": "categorical",
        "data_type": "VARCHAR",
        "on_null": "PASS",
        "default_value": "",
        "is_primary_key": False,
        "is_event_time": False,
        "filter_values": "[]",
        "on_filter": "PASS",
        "link_column": "",
        "link_mapping": "",
        "link_behaviour": "",
        "regex": "",
        "on_regex_mismatch": "FAIL",
        "source_unit": "",
        "target_unit": "",
        "scale_factor": "",
        "precision": "",
        "ordinal_position": 0,
    }
    row.update(overrides)
    return row


def height_row(**overrides):
    row_defaults = dict(
        column_name="height_m",
        var_type="numeric",
        data_type="DOUBLE",
        on_null="DEFAULT",
        default_value="9.0",
        ordinal_position=1,
    )
    row_defaults.update(overrides)
    return metadata_row(**row_defaults)


def leaf_type_row(**overrides):
    row_defaults = dict(
        column_name="leaf_type",
        var_type="numeric",
        data_type="INTEGER",
        on_null="DEFAULT",
        default_value="1",
        ordinal_position=2,
    )
    row_defaults.update(overrides)
    return metadata_row(**row_defaults)


def write_config(tmp_path, column_rows, bulk):
    """A single-dataset config ('trees') whose rows come from `bulk`."""
    pd.DataFrame(
        [
            {
                "schema": "source",
                "dataset": "trees",
                "file_regex": "dummy.csv",   # never read for bulk datasets
                "delim": "c",
                "distinct": "False",
            }
        ]
    ).to_csv(tmp_path / "file_metadata.tsv", sep="\t", index=False)

    pd.DataFrame(column_rows).to_csv(
        tmp_path / "column_metadata.tsv", sep="\t", index=False)

    return source_schema.Source(
        settings=source_schema.SourceSettings(
            file_metadata=source_schema.read_file_metadata(
                tmp_path / "file_metadata.tsv"
            ),
            expected_column_metadata=source_schema.read_column_metadata(
                tmp_path / "column_metadata.tsv"
            ),
            bulk=bulk,
        )
    )


def build_staging(source):
    return staging_schema.Staging(
        source=source,
        settings=staging_schema.StagingSettings(name="staging"),
    )


def init_connection(source, staging):
    conn = tyr.database.connections.Connection(
        name="test", syntax="duckdb", database=":memory:"
    )
    conn.execute(source.settings.sql)
    conn.execute(staging.settings.sql)

    registered = register_bulk_sources(conn.connection, staging)
    try:
        tyr.database.core.create_tables(source, conn)
        tyr.database.core.create_tables(staging, conn)
    finally:
        unregister_bulk_sources(conn.connection, registered)
    return conn


def trees_rows(conn):
    df = conn.execute(
        "SELECT species, height_m, leaf_type FROM staging.trees ORDER BY species"
    ).df()
    conn.close()
    return df


BULK_ROWS = [
    {"species": "oak", "height_m": None, "leaf_type": 0},
    {"species": "pine", "height_m": 12.5, "leaf_type": None},
    {"species": "yew", "height_m": None, "leaf_type": None},
]


def test_source_sql_references_bulk_placeholder(tmp_path):
    source = write_config(
        tmp_path, [metadata_row(), height_row(), leaf_type_row()],
        bulk={"trees": BULK_ROWS},
    )
    sql = source.tables.list_tables()[0].sql
    assert placeholder_name("trees") in sql
    assert "READ_CSV" not in sql
    assert "dummy.csv" not in sql


def test_bulk_exposed_on_schema(tmp_path):
    source = write_config(
        tmp_path, [metadata_row(), height_row(), leaf_type_row()],
        bulk={"trees": BULK_ROWS},
    )
    assert source.bulk_sources == {"trees": BULK_ROWS}
    staging = build_staging(source)
    # a Staging schema reaches the bulk map through its .source
    assert staging.source.bulk_sources == {"trees": BULK_ROWS}


def test_null_integer_survives_and_default_fills_only_nulls(tmp_path):
    """The INTEGER point of the exercise: pyarrow transports a real NULL in
    an int column; on_null=DEFAULT fills exactly the NULLs -- a genuine 0
    (oak: conifer) must NOT be overwritten by the default."""
    pa = pytest.importorskip("pyarrow")
    arrow = pa.table({
        "species": [r["species"] for r in BULK_ROWS],
        "height_m": [r["height_m"] for r in BULK_ROWS],
        "leaf_type": [r["leaf_type"] for r in BULK_ROWS],
    })
    source = write_config(
        tmp_path, [metadata_row(), height_row(), leaf_type_row()],
        bulk={"trees": arrow},
    )
    staging = build_staging(source)
    conn = init_connection(source, staging)
    df = trees_rows(conn)

    assert df["species"].tolist() == ["oak", "pine", "yew"]
    assert df["height_m"].tolist() == [9.0, 12.5, 9.0]
    assert df["leaf_type"].tolist() == [0, 1, 1]


def test_list_of_dicts_transport(tmp_path):
    """The convenience transport: None values in object columns survive as
    NULLs the same way."""
    source = write_config(
        tmp_path, [metadata_row(), height_row(), leaf_type_row()],
        bulk={"trees": BULK_ROWS},
    )
    staging = build_staging(source)
    conn = init_connection(source, staging)
    df = trees_rows(conn)

    assert df["height_m"].tolist() == [9.0, 12.5, 9.0]
    assert df["leaf_type"].tolist() == [0, 1, 1]


def test_bulk_dataset_coexists_with_file_dataset(tmp_path):
    """One schema, two ingestion mechanics: 'trees' bulk, 'notes' read from
    a real CSV file."""
    (tmp_path / "notes.csv").write_text("id,note\n1,hello\n2,world\n")
    pd.DataFrame(
        [
            {"schema": "source", "dataset": "trees", "file_regex": "dummy.csv",
             "delim": "c", "distinct": "False"},
            {"schema": "source", "dataset": "notes", "file_regex": "notes.csv",
             "delim": "c", "distinct": "False"},
        ]
    ).to_csv(tmp_path / "file_metadata.tsv", sep="\t", index=False)
    rows = [
        metadata_row(dataset="trees"),
        height_row(dataset="trees"),
        leaf_type_row(dataset="trees"),
        metadata_row(dataset="notes", column_name="id", var_type="numeric",
                     data_type="INTEGER", ordinal_position=0),
        metadata_row(dataset="notes", column_name="note", ordinal_position=1),
    ]
    pd.DataFrame(rows).to_csv(
        tmp_path / "column_metadata.tsv", sep="\t", index=False)

    source = source_schema.Source(
        settings=source_schema.SourceSettings(
            file_metadata=source_schema.read_file_metadata(
                tmp_path / "file_metadata.tsv"),
            expected_column_metadata=source_schema.read_column_metadata(
                tmp_path / "column_metadata.tsv"),
            bulk={"trees": BULK_ROWS},
        )
    )
    staging = build_staging(source)
    conn = init_connection(source, staging)
    df = conn.execute(
        "SELECT species, leaf_type FROM staging.trees ORDER BY species").df()
    notes = conn.execute(
        "SELECT id, note FROM staging.notes ORDER BY id").df()
    conn.close()

    assert df["leaf_type"].tolist() == [0, 1, 1]
    assert notes["note"].tolist() == ["hello", "world"]


def test_unregister_is_silent_and_repeatable(tmp_path):
    conn = tyr.database.connections.Connection(
        name="test", syntax="duckdb", database=":memory:")
    registered = register_bulk_sources(
        conn.connection,
        write_config(tmp_path, [metadata_row()], bulk={"trees": BULK_ROWS}))
    assert registered == [placeholder_name("trees")]
    unregister_bulk_sources(conn.connection, registered)
    unregister_bulk_sources(conn.connection, registered)   # no raise
    unregister_bulk_sources(conn.connection, ["never_registered"])  # no raise


def test_register_rejects_bad_transport(tmp_path):
    conn = tyr.database.connections.Connection(
        name="test", syntax="duckdb", database=":memory:")
    schema = write_config(
        tmp_path, [metadata_row()], bulk={"trees": 42})
    with pytest.raises(TypeError, match="pyarrow Table"):
        register_bulk_sources(conn.connection, schema)
