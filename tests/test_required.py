"""The required/optional table contract.

`required` is a boolean on the ``_Table`` parent class (default True: the
table MUST exist for the schema to be complete). It enters via the Source
file_metadata TSV's ``required`` column, propagates through every transform
(Select, the staging transform, clone_select, deepcopy), and is settable
directly on Core objects — e.g. an output-schema table whose staging source
is optional but which must (or must not) be enforced at validation time.

Semantics mirror apian tyrbuild's gate: only an explicit ``false``/``0``/
``no`` marks a table optional; absent column and blank cells mean required.
Older pickled schemas lack the attribute — consumers use
``getattr(table, "required", True)``.
"""

import copy

import pandas as pd

import tyr
from tyr.lineage import macros
from tyr.lineage import tables as lineage_tables
from tyr.lineage.schema import source as source_schema
from tyr.lineage.schema import staging as staging_schema

SPATIAL = [{"name": "spatial", "origin": "duckdb"}]

COLUMN_ROW = {
    "schema": "staging",
    "dataset": "roads",
    "column_name": "street_type",
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


def _write_area(tmp_path, required_cell="__ABSENT__"):
    """A one-file area; required_cell controls the file_metadata flag."""
    geojson = tmp_path / "roads.geojson"
    geojson.write_text(
        '{"type": "FeatureCollection", "features": []}', encoding="utf-8"
    )
    fm_row = {
        "schema": "staging",
        "dataset": "roads",
        "file_regex": str(geojson),
        "delim": "c",
        "distinct": "False",
    }
    if required_cell != "__ABSENT__":
        fm_row["required"] = required_cell
    pd.DataFrame([fm_row]).to_csv(
        tmp_path / "file_metadata.tsv", sep="\t", index=False
    )
    pd.DataFrame([COLUMN_ROW]).to_csv(
        tmp_path / "column_metadata.tsv", sep="\t", index=False
    )
    return tmp_path


def _source_staging(tmp_path, required_cell="__ABSENT__"):
    tmp_path = _write_area(tmp_path, required_cell)
    source = source_schema.Source(
        settings=source_schema.SourceSettings(
            file_metadata=source_schema.read_file_metadata(
                tmp_path / "file_metadata.tsv"
            ),
            expected_column_metadata=source_schema.read_column_metadata(
                tmp_path / "column_metadata.tsv"
            ),
            extensions=SPATIAL,
        )
    )
    staging = staging_schema.Staging(
        source=source,
        settings=staging_schema.StagingSettings(name="staging", extensions=SPATIAL),
    )
    return source, staging


# ── file_metadata TSV flag ────────────────────────────────────────────────────


def test_file_metadata_required_flag_parses(tmp_path):
    assert _source_staging(tmp_path, "False")[0].tables["roads"].required is False
    assert _source_staging(tmp_path, "0")[0].tables["roads"].required is False
    assert _source_staging(tmp_path, "no")[0].tables["roads"].required is False
    assert _source_staging(tmp_path, "True")[0].tables["roads"].required is True
    # Blank cell and absent column both mean required (the default).
    assert _source_staging(tmp_path, "")[0].tables["roads"].required is True
    assert _source_staging(tmp_path)[0].tables["roads"].required is True


# ── propagation ───────────────────────────────────────────────────────────────


def test_staging_transform_inherits_required(tmp_path):
    _source, staging = _source_staging(tmp_path, "False")
    assert staging.tables["roads"].required is False


def test_clone_select_inherits_and_overrides(tmp_path):
    _source, staging = _source_staging(tmp_path, "False")
    src = lineage_tables.Select(staging.tables["roads"])
    inherited = macros.tables.clone_select(src)
    assert inherited.required is False
    overridden = macros.tables.clone_select(src, required=True)
    assert overridden.required is True


def test_select_wraps_required(tmp_path):
    _source, staging = _source_staging(tmp_path, "False")
    assert lineage_tables.Select(staging.tables["roads"]).required is False


def test_core_direct_and_default(tmp_path):
    from tyr.lineage.core import ColumnList
    from tyr.lineage.macros.columns import select_all

    _source, staging = _source_staging(tmp_path)          # required defaults True
    assert staging.tables["roads"].required is True

    src = lineage_tables.Select(staging.tables["roads"])
    explicit = lineage_tables.Core(
        name="roads_out",
        columns=select_all(src),
        source=src,
        required=False,
    )
    assert explicit.required is False


def test_deepcopy_preserves_required(tmp_path):
    _source, staging = _source_staging(tmp_path, "False")
    clone = copy.deepcopy(staging.tables["roads"])
    assert clone.required is False


def test_output_schema_sets_required_directly(tmp_path):
    # The consumer-facing case: an output schema built from staging tables,
    # with the contract set per table after building.
    _source, staging = _source_staging(tmp_path, "False")
    project = tyr.lineage.schema.project.Project(
        tyr.lineage.schema.project.ProjectSettings(name="output")
    )
    out = macros.tables.clone_select(lineage_tables.Select(staging.tables["roads"]))
    out.required = True   # optional source, but the output MUST exist
    project.add_table(out)
    assert project.tables["roads"].required is True
    assert staging.tables["roads"].required is False   # source unchanged
