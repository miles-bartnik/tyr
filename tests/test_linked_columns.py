"""Linked columns: the third staging value-transform phase.

A column may link to another column in the same dataset; after the null-handling
phase and filtering, a final CaseWhen inside the staging SELECT substitutes a
mapped value based on the link column's transformed value in the same row.
"""

import pandas as pd
import pytest
import tyr

from tyr.lineage import core as lineage
from tyr.lineage.macros.columns import parse_link_mapping, validate_column_links
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


def column_metadata(**overrides):
    return source_schema.ColumnMetadata(pd.Series(metadata_row(**overrides)))


def write_config(tmp_path, csv_text, column_rows):
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


KEY_ROW = metadata_row(column_name="key", ordinal_position=0)


def value_row(**overrides):
    row_defaults = dict(
        column_name="value",
        var_type="numeric",
        data_type="INTEGER",
        ordinal_position=1,
    )
    row_defaults.update(overrides)
    return metadata_row(**row_defaults)


LINKED_VALUE_ROW = value_row(
    link_column="key",
    link_mapping='{"A":"1","B":"2"}',
    link_behaviour="ON_NULL",
)

CSV = "key,value\nA,\nB,7\n,5\n"


# parse_link_mapping


def test_parse_link_mapping_dict_only():
    assert parse_link_mapping('{"A":"value_1", "B":"value_2"}') == (
        {"A": "value_1", "B": "value_2"},
        None,
    )


def test_parse_link_mapping_with_fallback_segment():
    assert parse_link_mapping('{"A":"value_1"} | "null_value"') == (
        {"A": "value_1"},
        "null_value",
    )


def test_parse_link_mapping_fallback_segment_quotes_optional():
    assert parse_link_mapping("""{"A":"value_1"} | 'null_value'""") == (
        {"A": "value_1"},
        "null_value",
    )
    assert parse_link_mapping('{"A":"value_1"} | null_value') == (
        {"A": "value_1"},
        "null_value",
    )


def test_parse_link_mapping_malformed():
    with pytest.raises(ValueError):
        parse_link_mapping("not a dict")
    with pytest.raises(ValueError):
        parse_link_mapping('["A"]')
    with pytest.raises(ValueError):
        parse_link_mapping('{"A":1}')
    with pytest.raises(ValueError):
        parse_link_mapping("")
    with pytest.raises(ValueError):
        parse_link_mapping('{"A":"1"} | ')


# Build-time validation


def test_validation_passes_for_valid_config():
    validate_column_links(
        {
            "key": column_metadata(**KEY_ROW),
            "value": column_metadata(**LINKED_VALUE_ROW),
        }
    )


def test_validation_link_mapping_required_when_link_column_set():
    with pytest.raises(ValueError, match="link_mapping is required"):
        validate_column_links(
            {"value": column_metadata(**value_row(link_column="key"))}
        )


def test_validation_link_mapping_malformed():
    with pytest.raises(ValueError, match="malformed"):
        validate_column_links(
            {
                "value": column_metadata(
                    **value_row(link_column="key", link_mapping="not json")
                )
            }
        )


def test_validation_link_mapping_without_link_column():
    with pytest.raises(ValueError, match="link_column is empty"):
        validate_column_links(
            {"value": column_metadata(**value_row(link_mapping='{"A":"1"}'))}
        )


@pytest.mark.parametrize("link_behaviour", ["", "ALWAYS", "on_null"])
def test_validation_link_behaviour_invalid(link_behaviour):
    with pytest.raises(ValueError, match="ON_NULL or OVERWRITE"):
        validate_column_links(
            {
                "value": column_metadata(
                    **value_row(
                        link_column="key",
                        link_mapping='{"A":"1"}',
                        link_behaviour=link_behaviour,
                    )
                )
            }
        )


def test_validation_link_column_does_not_exist():
    with pytest.raises(ValueError, match="does not exist"):
        validate_column_links(
            {
                "key": column_metadata(**KEY_ROW),
                "value": column_metadata(
                    **value_row(
                        link_column="nope",
                        link_mapping='{"A":"1"}',
                        link_behaviour="ON_NULL",
                    )
                ),
            }
        )


def test_validation_link_column_with_skip_filter_rejected():
    with pytest.raises(ValueError, match="on_filter=SKIP"):
        validate_column_links(
            {
                "key": column_metadata(
                    **{**KEY_ROW, "filter_values": '["X"]', "on_filter": "SKIP"}
                ),
                "value": column_metadata(**LINKED_VALUE_ROW),
            }
        )


def test_validation_link_column_with_skip_null_rejected():
    with pytest.raises(ValueError, match="on_null=SKIP"):
        validate_column_links(
            {
                "key": column_metadata(**{**KEY_ROW, "on_null": "SKIP"}),
                "value": column_metadata(**LINKED_VALUE_ROW),
            }
        )


def test_validation_direct_cycle_rejected():
    with pytest.raises(ValueError, match="Cycle"):
        validate_column_links(
            {
                "a": column_metadata(
                    **value_row(
                        column_name="a",
                        link_column="b",
                        link_mapping='{"B":"1"}',
                        link_behaviour="OVERWRITE",
                    )
                ),
                "b": column_metadata(
                    **value_row(
                        column_name="b",
                        link_column="a",
                        link_mapping='{"A":"1"}',
                        link_behaviour="OVERWRITE",
                    )
                ),
            }
        )


def test_validation_indirect_cycle_rejected():
    with pytest.raises(ValueError, match="Cycle"):
        validate_column_links(
            {
                name: column_metadata(
                    **value_row(
                        column_name=name,
                        link_column=target,
                        link_mapping='{"A":"1"}',
                        link_behaviour="OVERWRITE",
                    )
                )
                for name, target in [("a", "b"), ("b", "c"), ("c", "a")]
            }
        )


def test_validation_raises_when_staging_is_built(tmp_path):
    source = write_config(
        tmp_path,
        CSV,
        [
            KEY_ROW,
            value_row(
                link_column="value",  # self-link: a 1-cycle
                link_mapping='{"A":"1"}',
                link_behaviour="ON_NULL",
            ),
        ],
    )

    with pytest.raises(ValueError, match="Cycle"):
        build_staging(source)


# Generated SQL structure


def test_link_case_when_wraps_after_null_handling(tmp_path):
    source = write_config(
        tmp_path,
        CSV,
        [
            KEY_ROW,
            value_row(
                on_null="DEFAULT",
                default_value="-999",
                link_column="key",
                link_mapping='{"A":"111"} | "222"',
                link_behaviour="ON_NULL",
            ),
        ],
    )
    staging = build_staging(source)

    column = staging.tables["links"].columns["value"]

    # The link phase is a final CaseWhen wrapper around the (null-handled) column.
    assert isinstance(column.source, lineage.CaseWhen)

    link_case = column.source.values[0]
    assert isinstance(link_case, lineage.CaseWhen)
    assert "ERROR" in link_case.else_value.sql
    # NULL key hits the fallback segment first.
    assert column.sql.index("'222'") < column.sql.index("'111'")

    # The wrapped (else) branch is the null-handling expression, i.e. the link
    # transform is applied after the null-handling phase, in the same SELECT.
    assert "-999" in column.source.else_value.sql
    assert isinstance(column.source.else_value.source, lineage.CaseWhen)

    # Ordering in the compiled SQL: substitution appears after the null default.
    assert column.sql.index("'111'") > column.sql.index("-999")


def test_no_link_config_leaves_column_unwrapped(tmp_path):
    source = write_config(tmp_path, CSV, [KEY_ROW, value_row()])
    staging = build_staging(source)

    assert "Link key" not in staging.tables["links"].sql


# Execution semantics (DuckDB)


def test_on_null_fires_only_on_null(tmp_path):
    source = write_config(tmp_path, CSV, [KEY_ROW, LINKED_VALUE_ROW])
    staging = build_staging(source)
    conn = init_connection(source, staging)

    result = conn.execute(
        "SELECT key, value FROM staging.links ORDER BY key NULLS LAST, value"
    ).df()

    assert result["value"].tolist() == [1, 7, 5]

    conn.close()


def test_overwrite_always_fires(tmp_path):
    source = write_config(
        tmp_path,
        CSV,
        [
            KEY_ROW,
            value_row(
                link_column="key",
                link_mapping='{"A":"1","B":"2"}',
                link_behaviour="OVERWRITE",
            ),
        ],
    )
    staging = build_staging(source)
    conn = init_connection(source, staging)

    result = conn.execute(
        "SELECT key, value FROM staging.links ORDER BY key NULLS LAST, value"
    ).df()

    assert result["value"].tolist() == [1, 2, 5]

    conn.close()


def test_null_link_key_substitutes_null_value_segment(tmp_path):
    source = write_config(
        tmp_path,
        "key,value\n,\nA,\n",
        [
            KEY_ROW,
            value_row(
                link_column="key",
                link_mapping='{"A":"1"} | "9"',
                link_behaviour="ON_NULL",
            ),
        ],
    )
    staging = build_staging(source)
    conn = init_connection(source, staging)

    result = conn.execute(
        "SELECT key, value FROM staging.links ORDER BY value"
    ).df()

    assert result["value"].tolist() == [1, 9]

    conn.close()


def test_key_miss_raises_runtime_error(tmp_path):
    source = write_config(
        tmp_path,
        "key,value\nZ,\n",
        [KEY_ROW, LINKED_VALUE_ROW],
    )
    staging = build_staging(source)
    conn = tyr.database.connections.Connection(
        name="test", syntax="duckdb", database=":memory:"
    )

    conn.execute(source.settings.sql)
    conn.execute(staging.settings.sql)
    tyr.database.core.create_tables(source, conn)

    with pytest.raises(Exception, match="Link key"):
        tyr.database.core.create_tables(staging, conn)

    conn.close()


def test_cast_failure_raises_runtime_error(tmp_path):
    source = write_config(
        tmp_path,
        "key,value\nA,\n",
        [
            KEY_ROW,
            value_row(
                link_column="key",
                link_mapping='{"A":"not_a_number"}',
                link_behaviour="ON_NULL",
            ),
        ],
    )
    staging = build_staging(source)
    conn = tyr.database.connections.Connection(
        name="test", syntax="duckdb", database=":memory:"
    )

    conn.execute(source.settings.sql)
    conn.execute(staging.settings.sql)
    tyr.database.core.create_tables(source, conn)

    with pytest.raises(Exception, match="not_a_number"):
        tyr.database.core.create_tables(staging, conn)

    conn.close()


# Metadata plumbing


def test_metadata_round_trip(tmp_path):
    path = tmp_path / "column_metadata.tsv"
    pd.DataFrame(
        [
            metadata_row(),
            value_row(
                link_column="key",
                link_mapping='{"A":"1","B":"2"} | "9"',
                link_behaviour="OVERWRITE",
            ),
        ]
    ).to_csv(path, sep="\t", index=False)

    parsed = source_schema.read_column_metadata(path)["links"]

    assert parsed["key"].link_column == ""
    assert parsed["key"].link_mapping == ""
    assert parsed["key"].link_behaviour == ""
    assert parsed["value"].link_column == "key"
    assert parsed["value"].link_mapping == '{"A":"1","B":"2"} | "9"'
    assert parsed["value"].link_behaviour == "OVERWRITE"


def test_metadata_file_without_link_fields_defaults_empty(tmp_path):
    path = tmp_path / "column_metadata.tsv"
    pd.DataFrame([metadata_row(), value_row()]).drop(
        columns=["link_column", "link_mapping", "link_behaviour"]
    ).to_csv(path, sep="\t", index=False)

    parsed = source_schema.read_column_metadata(path)["links"]

    assert parsed["key"].link_column == ""
    assert parsed["value"].link_mapping == ""


def test_init_column_metadata_writes_link_fields(tmp_path):
    path = tmp_path / "column_metadata.tsv"
    source_schema.init_column_metadata(path=path)

    header = path.read_text().splitlines()[0].split("\t")

    assert ["link_column", "link_mapping", "link_behaviour"] <= header
