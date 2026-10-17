"""on_regex_mismatch column policy: FAIL aborts, NULL nulls the cell, SKIP drops
the row; empty regex means no check; on_regex_mismatch=NULL on a primary key is a
build-time error; metadata files written before the field existed still parse.

Helpers mirror tests/test_staging_skip_and_schema.py (self-contained copy so the
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
        "on_regex_mismatch": "FAIL",
        "source_unit": "",
        "target_unit": "",
        "scale_factor": "",
        "precision": "",
        "ordinal_position": 0,
    }
    row.update(overrides)
    return row


def timestamp_row(**overrides):
    row_defaults = dict(
        column_name="when_date",
        var_type="timestamp",
        data_type="TIMESTAMP",
        regex="%m/%d/%Y",
        ordinal_position=1,
    )
    row_defaults.update(overrides)
    return metadata_row(**row_defaults)


def word_row(**overrides):
    row_defaults = dict(
        column_name="word",
        var_type="string",
        data_type="VARCHAR",
        regex="^[a-z]+$",
        ordinal_position=2,
    )
    row_defaults.update(overrides)
    return metadata_row(**row_defaults)


CSV_TEXT = "key,when_date,word\nA,01/02/2020,hello\nB,2020-01-02,world\nC,,again\n"


def write_config(tmp_path, column_rows, csv_text=CSV_TEXT, omit_field=False):
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

    df = pd.DataFrame(column_rows)
    if omit_field and "on_regex_mismatch" in df.columns:
        df = df.drop(columns=["on_regex_mismatch"])
    df.to_csv(tmp_path / "column_metadata.tsv", sep="\t", index=False)

    source = source_schema.Source(
        settings=source_schema.SourceSettings(
            file_metadata=source_schema.read_file_metadata(
                tmp_path / "file_metadata.tsv"
            ),
            expected_column_metadata=source_schema.read_column_metadata(
                tmp_path / "column_metadata.tsv"
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


def staged_rows(conn):
    df = conn.execute(
        "SELECT key, when_date, word FROM staging.links ORDER BY key"
    ).df()
    conn.close()
    return df


# Timestamp column (strptime form)


def test_timestamp_fail_aborts_on_mismatch(tmp_path):
    source = write_config(
        tmp_path,
        [metadata_row(), timestamp_row(on_regex_mismatch="FAIL"), word_row()],
    )
    staging = build_staging(source)

    # The staging build itself raises (CREATE TABLE evaluates the transform).
    with pytest.raises(Exception, match="links.when_date"):
        init_connection(source, staging)


def test_timestamp_null_nulls_mismatch_and_keeps_row(tmp_path):
    source = write_config(
        tmp_path,
        [metadata_row(), timestamp_row(on_regex_mismatch="NULL"), word_row()],
    )
    staging = build_staging(source)
    conn = init_connection(source, staging)
    df = staged_rows(conn)

    # B's ISO straggler is nulled; the row survives alongside the NULL row C.
    assert df["key"].tolist() == ["A", "B", "C"]
    assert df.loc[df["key"] == "A", "when_date"].notna().all()
    assert df.loc[df["key"] == "B", "when_date"].isna().all()
    assert df.loc[df["key"] == "C", "when_date"].isna().all()


def test_timestamp_skip_drops_mismatching_row(tmp_path):
    source = write_config(
        tmp_path,
        [metadata_row(), timestamp_row(on_regex_mismatch="SKIP"), word_row()],
    )
    staging = build_staging(source)
    conn = init_connection(source, staging)
    df = staged_rows(conn)

    # B (bad format) is dropped; C (NULL) survives.
    assert df["key"].tolist() == ["A", "C"]


def test_timestamp_skip_where_admits_null_and_uses_try_strptime(tmp_path):
    source = write_config(
        tmp_path,
        [metadata_row(), timestamp_row(on_regex_mismatch="SKIP"), word_row()],
    )
    staging = build_staging(source)

    sql = staging.tables["links"].sql
    assert "TRY_STRPTIME" in sql
    assert "WHERE" in sql
    assert "IS NOT NULL" in sql
    assert "OR" in sql


# Varchar column (regexp_matches form)


def test_varchar_fail_aborts_on_mismatch(tmp_path):
    source = write_config(
        tmp_path,
        [
            metadata_row(),
            timestamp_row(var_type="string", data_type="VARCHAR", regex=""),
            word_row(on_regex_mismatch="FAIL"),
        ],
        csv_text="key,when_date,word\nA,01/02/2020,hello\nB,01/03/2020,HAS SPACE\n",
    )
    staging = build_staging(source)

    # The staging build itself raises (CREATE TABLE evaluates the transform).
    with pytest.raises(Exception, match="links.word"):
        init_connection(source, staging)


def test_varchar_null_nulls_mismatch_and_keeps_row(tmp_path):
    source = write_config(
        tmp_path,
        [
            metadata_row(),
            timestamp_row(var_type="string", data_type="VARCHAR", regex=""),
            word_row(on_regex_mismatch="NULL"),
        ],
        csv_text="key,when_date,word\nA,01/02/2020,hello\nB,01/03/2020,HAS SPACE\nC,01/04/2020,\n",
    )
    staging = build_staging(source)
    conn = init_connection(source, staging)
    df = staged_rows(conn)

    assert df["key"].tolist() == ["A", "B", "C"]
    assert df.loc[df["key"] == "B", "word"].isna().all()
    assert df.loc[df["key"] == "A", "word"].tolist() == ["hello"]


def test_varchar_skip_drops_mismatching_row(tmp_path):
    source = write_config(
        tmp_path,
        [
            metadata_row(),
            timestamp_row(var_type="string", data_type="VARCHAR", regex=""),
            word_row(on_regex_mismatch="SKIP"),
        ],
        csv_text="key,when_date,word\nA,01/02/2020,hello\nB,01/03/2020,HAS SPACE\nC,01/04/2020,\n",
    )
    staging = build_staging(source)
    conn = init_connection(source, staging)
    df = staged_rows(conn)

    assert df["key"].tolist() == ["A", "C"]


def test_varchar_skip_where_uses_regexp_matches(tmp_path):
    source = write_config(
        tmp_path,
        [
            metadata_row(),
            timestamp_row(var_type="string", data_type="VARCHAR", regex=""),
            word_row(on_regex_mismatch="SKIP"),
        ],
        csv_text="key,when_date,word\nA,01/02/2020,hello\n",
    )
    staging = build_staging(source)

    sql = staging.tables["links"].sql
    assert "REGEXP_MATCHES" in sql
    assert "WHERE" in sql


# No regex -> no check at all


def test_empty_regex_means_no_check(tmp_path):
    source = write_config(
        tmp_path,
        [
            metadata_row(),
            timestamp_row(regex="", on_regex_mismatch="FAIL"),
            word_row(regex="", on_regex_mismatch="FAIL"),
        ],
        csv_text="key,when_date,word\nA,2020-01-02 00:00:00,HAS SPACE\n",
    )
    staging = build_staging(source)
    conn = init_connection(source, staging)
    df = staged_rows(conn)

    assert df["key"].tolist() == ["A"]
    assert df.loc[df["key"] == "A", "word"].tolist() == ["HAS SPACE"]


def test_no_regex_mismatch_clause_rendered_without_regex(tmp_path):
    source = write_config(
        tmp_path,
        [
            metadata_row(),
            timestamp_row(regex="", on_regex_mismatch="FAIL"),
            word_row(regex="", on_regex_mismatch="FAIL"),
        ],
    )
    staging = build_staging(source)
    sql = staging.tables["links"].sql

    assert "regex mismatch" not in sql
    assert "TRY_STRPTIME" not in sql
    assert "REGEXP_MATCHES" not in sql


# Compile-time conflict: NULL policy on a primary key


def test_null_policy_on_primary_key_raises_build_error(tmp_path):
    source = write_config(
        tmp_path,
        [
            metadata_row(is_primary_key=True, on_regex_mismatch="NULL"),
            timestamp_row(regex=""),
            word_row(regex=""),
        ],
    )

    with pytest.raises(ValueError, match="links.key"):
        build_staging(source)


# Metadata written before the field existed


def test_metadata_without_the_field_defaults_fail(tmp_path):
    path = tmp_path / "column_metadata.tsv"
    pd.DataFrame([metadata_row(), timestamp_row()]).drop(
        columns=["on_regex_mismatch"]
    ).to_csv(path, sep="\t", index=False)

    parsed = source_schema.read_column_metadata(path)

    assert parsed["links"]["key"].on_regex_mismatch == "FAIL"
    assert parsed["links"]["when_date"].on_regex_mismatch == "FAIL"


def test_empty_cell_defaults_fail(tmp_path):
    path = tmp_path / "column_metadata.tsv"
    pd.DataFrame(
        [metadata_row(), timestamp_row(on_regex_mismatch="")]
    ).to_csv(path, sep="\t", index=False)

    parsed = source_schema.read_column_metadata(path)

    assert parsed["links"]["when_date"].on_regex_mismatch == "FAIL"


def test_policy_values_round_trip_through_tsv(tmp_path):
    path = tmp_path / "column_metadata.tsv"
    pd.DataFrame(
        [
            metadata_row(on_regex_mismatch="SKIP"),
            timestamp_row(on_regex_mismatch="NULL"),
        ]
    ).to_csv(path, sep="\t", index=False)

    parsed = source_schema.read_column_metadata(path)

    assert parsed["links"]["key"].on_regex_mismatch == "SKIP"
    assert parsed["links"]["when_date"].on_regex_mismatch == "NULL"


def test_init_column_metadata_writes_the_field(tmp_path):
    df = source_schema.init_column_metadata()

    assert "on_regex_mismatch" in df.columns
    assert list(df.columns).index("on_regex_mismatch") == (
        list(df.columns).index("regex") + 1
    )
