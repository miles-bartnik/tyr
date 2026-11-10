""".json source routing: GeoJSON FeatureCollections keep the ST_Read path;
a plain JSON records file (top-level array of objects -- the Apian
vegetation catalog's shape) stages via DuckDB's read_json_auto.

The routing peeks the file CONTENT, not just the extension: both shapes
carry .json names (the catalog sidecar is <area>_vegetation_catalog.json).
"""

import json

import duckdb
import pandas as pd

from tyr.lineage.schema import source as source_schema


def _write_records(tmp_path):
    path = tmp_path / "vegetation_catalog.json"
    path.write_text(
        json.dumps([
            {"species_id": "norfolk_island_pine", "state": "include", "quota": 3},
            {"species_id": "bracken_fern", "state": None},
        ]),
        encoding="utf-8",
    )
    return path


def _write_featurecollection(tmp_path):
    path = tmp_path / "roads.json"
    path.write_text(
        json.dumps({
            "type": "FeatureCollection",
            "features": [{
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [7.4, 43.7]},
                "properties": {"name": "main"},
            }],
        }),
        encoding="utf-8",
    )
    return path


def _source_for(tmp_path, path):
    pd.DataFrame(
        [{
            "schema": "staging",
            "dataset": path.stem,
            "file_regex": str(path),
            "delim": "c",
            "distinct": "False",
        }]
    ).to_csv(tmp_path / "file_metadata.tsv", sep="\t", index=False)
    row = {c: "" for c in [
        "schema", "dataset", "column_name", "column_alias", "var_type",
        "data_type", "on_null", "default_value", "is_primary_key",
        "is_event_time", "filter_values", "on_filter", "regex",
        "on_regex_mismatch", "source_unit", "target_unit", "scale_factor",
        "precision", "ordinal_position", "link_column", "link_mapping",
        "link_behaviour",
    ]}
    row.update({
        "schema": "staging",
        "dataset": path.stem,
        "column_name": "species_id" if "catalog" in path.stem else "name",
        "var_type": "categorical",
        "data_type": "VARCHAR",
        "on_null": "PASS",
        "on_filter": "PASS",
        "on_regex_mismatch": "FAIL",
        "filter_values": "[]",
        "ordinal_position": 0,
    })
    pd.DataFrame([row]).to_csv(
        tmp_path / "column_metadata.tsv", sep="\t", index=False
    )
    return source_schema.Source(
        settings=source_schema.SourceSettings(
            file_metadata=source_schema.read_file_metadata(
                tmp_path / "file_metadata.tsv"
            ),
            expected_column_metadata=source_schema.read_column_metadata(
                tmp_path / "column_metadata.tsv"
            ),
            extensions=[],
        )
    )


def test_records_json_routes_to_read_json_auto(tmp_path):
    src = _source_for(tmp_path, _write_records(tmp_path))
    table = next(t for t in src.tables.list_tables()
                 if t.name == "vegetation_catalog")
    assert "read_json_auto" in table.sql
    assert "ST_READ" not in table.sql.upper().replace("READ_JSON_AUTO", "")
    # the SQL is executable against the real file
    con = duckdb.connect()
    try:
        rows = con.execute(table.sql).fetchall()
    finally:
        con.close()
    assert len(rows) == 2


def test_featurecollection_json_keeps_st_read(tmp_path):
    src = _source_for(tmp_path, _write_featurecollection(tmp_path))
    table = next(t for t in src.tables.list_tables() if t.name == "roads")
    assert "ST_READ" in table.sql.upper()
    assert "read_json_auto" not in table.sql


def test_json_records_materialises_into_duckdb(tmp_path):
    src = _source_for(tmp_path, _write_records(tmp_path))
    table = next(t for t in src.tables.list_tables()
                 if t.name == "vegetation_catalog")
    con = duckdb.connect()
    try:
        con.execute(f"CREATE TABLE t AS {table.sql}")
        got = con.execute(
            "SELECT species_id, quota FROM t ORDER BY species_id").fetchall()
    finally:
        con.close()
    assert got == [("bracken_fern", None), ("norfolk_island_pine", 3)]
