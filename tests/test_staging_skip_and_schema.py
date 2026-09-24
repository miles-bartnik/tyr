"""Staging-validation fixes: on_filter=SKIP must keep NULL rows, and
read_column_metadata must be able to exclude rows tagged for a foreign schema.

Helpers mirror tests/test_linked_columns.py (self-contained copy so the two
files stay independently runnable).
"""

import pandas as pd
import pytest
import tyr

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
    "source_unit",
    "target_unit",
    "scale_factor",
    "precision",
    "ordinal_position",
]


def metadata_row(**overrides):
    row = {
        "schema": "source",
        "dataset": "links",
        "column_name": "key",
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
        "source_unit": "",
        "target_unit": "",
        "scale_factor": "",
        "precision": "",
        "ordinal_position": 0,
    }
    row.update(overrides)
    return row


def value_row(**overrides):
    row_defaults = dict(
        column_name="value",
        var_type="numeric",
        data_type="INTEGER",
        ordinal_position=1,
    )
    row_defaults.update(overrides)
    return metadata_row(**row_defaults)


def write_config(tmp_path, csv_text, column_rows, read_schema=None):
    (tmp_path / "links.csv").write_text(csv_text)

    pd.DataFrame(
        [
            {
                "schema": "source",
                "dataset": "links",
                "file_regex": "links.csv",
                "delim": "c",
                "distinct": "False",
            }
        ]
    ).to_csv(tmp_path / "file_metadata.tsv", sep="\t", index=False)

    pd.DataFrame(column_rows).to_csv(
        tmp_path / "column_metadata.tsv", sep="\t", index=False
    )

    read_kwargs = {} if read_schema is None else {"schema": read_schema}
    source = source_schema.Source(
        settings=source_schema.SourceSettings(
            file_metadata=source_schema.read_file_metadata(
                tmp_path / "file_metadata.tsv"
            ),
            expected_column_metadata=source_schema.read_column_metadata(
                tmp_path / "column_metadata.tsv", **read_kwargs
            ),
        )
    )

    return source


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

    tyr.database.core.create_tables(source, conn)
    tyr.database.core.create_tables(staging, conn)

    return conn


# on_filter=SKIP keeps NULL rows


@pytest.mark.parametrize(
    "filter_values",
    ['["9"]', '["9", "42"]'],
    ids=["single-value-notequal", "multi-value-notin"],
)
def test_on_filter_skip_keeps_null_rows(tmp_path, filter_values):
    source = write_config(
        tmp_path,
        csv_text="key,value\nA,1\nB,9\nC,\nD,7\n",
        column_rows=[
            metadata_row(),
            value_row(on_filter="SKIP", filter_values=filter_values),
        ],
    )
    staging = build_staging(source)
    conn = init_connection(source, staging)
    df = conn.execute("SELECT key, value FROM staging.links ORDER BY key").df()
    conn.close()

    # "9" rows are skipped, everything else -- including the NULL row C --
    # survives: NULL contradicts on_null=PASS, it is not a filter value.
    assert df["key"].tolist() == ["A", "C", "D"]
    assert df.loc[df["key"] == "C", "value"].isna().all()


def test_skip_check_admits_null_in_generated_sql(tmp_path):
    source = write_config(
        tmp_path,
        csv_text="key,value\nA,1\n",
        column_rows=[
            metadata_row(),
            value_row(on_filter="SKIP", filter_values='["9"]'),
        ],
    )
    staging = build_staging(source)

    sql = staging.tables["links"].sql
    assert "IS NULL" in sql
    # The NULL admission is OR-ed with the not-equal check; it must be
    # parenthesised so a second check doesn't bind the AND first.
    or_pos = sql.index(" OR ")
    assert sql.rindex("(", 0, or_pos) > sql.index("WHERE")


# read_column_metadata honours the schema column


def test_read_column_metadata_schema_filter_excludes_foreign_rows(tmp_path):
    path = tmp_path / "column_metadata.tsv"
    # The foreign-schema row comes LAST so an unfiltered read picks it up
    # (dict comprehension: last row wins) -- that is the leak being fixed.
    pd.DataFrame(
        [
            metadata_row(schema="staging"),
            value_row(schema="staging", on_null="PASS"),
            value_row(schema="output", on_null="SKIP"),
        ]
    ).to_csv(path, sep="\t", index=False)

    parsed = source_schema.read_column_metadata(path, schema="staging")

    assert "value" in parsed["links"]
    assert parsed["links"]["value"].on_null == "PASS"

    # No schema argument: legacy behaviour, every row drives the build.
    unfiltered = source_schema.read_column_metadata(path)
    assert unfiltered["links"]["value"].on_null == "SKIP"


def test_read_column_metadata_schema_filter_keeps_empty_schema_rows(tmp_path):
    path = tmp_path / "column_metadata.tsv"
    pd.DataFrame(
        [
            metadata_row(schema=""),
            value_row(schema="output", on_null="SKIP"),
        ]
    ).to_csv(path, sep="\t", index=False)

    parsed = source_schema.read_column_metadata(path, schema="staging")

    assert parsed["links"]["key"].on_null == "PASS"
    assert "value" not in parsed["links"]


def test_read_column_metadata_schema_filter_drops_fully_foreign_dataset(tmp_path):
    path = tmp_path / "column_metadata.tsv"
    pd.DataFrame(
        [
            metadata_row(schema="staging"),
            metadata_row(schema="output", dataset="ghost", ordinal_position=0),
        ]
    ).to_csv(path, sep="\t", index=False)

    parsed = source_schema.read_column_metadata(path, schema="staging")

    assert "links" in parsed
    assert "ghost" not in parsed


def test_foreign_schema_row_does_not_drive_staging_transform(tmp_path):
    # Same dataset/column under schema "staging" (on_null=PASS) and schema
    # "output" (on_null=SKIP, foreign). Reading for the staging build must
    # exclude the foreign row, so the NULL value survives staging.
    source = write_config(
        tmp_path,
        csv_text="key,value\nA,1\nB,\n",
        column_rows=[
            metadata_row(schema="staging"),
            value_row(schema="staging", on_null="PASS"),
            value_row(schema="output", on_null="SKIP"),
        ],
        read_schema="staging",
    )
    staging = build_staging(source)
    conn = init_connection(source, staging)
    df = conn.execute("SELECT key, value FROM staging.links ORDER BY key").df()
    conn.close()

    assert df["key"].tolist() == ["A", "B"]
