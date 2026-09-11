"""GeoJSON column typing and the staging type's lineage.

Regression (apian review T6, decision D6): when DuckDB's spatial extension
could not read a GeoJSON, tyr's fallback sniffer typed every property VARCHAR
without looking at the values, so tyr-produced column metadata could disagree
with the data a column actually carries (a numeric street `width_m` sniffed
as VARCHAR). The fallback now infers a type from the decoded JSON values, and
the staging build carries the declared type across the wildcard bridge in
lineage so a consumer can see where the staged column's type came from.
"""

import json

import pandas as pd

import tyr
from tyr.lineage import values as lineage_values
from tyr.lineage.schema import source as source_schema
from tyr.lineage.schema import staging as staging_schema

SPATIAL = [{"name": "spatial", "origin": "duckdb"}]


def _write_geojson(tmp_path, properties_by_feature):
    path = tmp_path / "roads.geojson"
    path.write_text(
        json.dumps(
            {
                "type": "FeatureCollection",
                "features": [
                    {
                        "type": "Feature",
                        "geometry": {"type": "Point", "coordinates": [7.4, 43.7]},
                        "properties": properties,
                    }
                    for properties in properties_by_feature
                ],
            }
        ),
        encoding="utf-8",
    )
    return path


# _geojson_property_columns: value-based inference


def test_geojson_property_columns_infer_value_types(tmp_path):
    path = _write_geojson(
        tmp_path,
        [
            {
                "name": "Main St",            # string -> VARCHAR
                "width_m": 3.5,               # fractional -> DOUBLE
                "lanes": 2,                   # integral -> INTEGER
                "tunnel": False,              # boolean -> BOOLEAN
                "unparsed": None,             # null-only -> VARCHAR
            },
            {
                "name": "Side St",
                "width_m": 4,                 # int + float promote to DOUBLE
                "lanes": 1,
                "tunnel": True,
                "unparsed": None,
            },
        ],
    )

    columns = dict(source_schema._geojson_property_columns(str(path)))

    assert columns["name"] == "VARCHAR"
    assert columns["width_m"] == "DOUBLE"
    assert columns["lanes"] == "INTEGER"
    assert columns["tunnel"] == "BOOLEAN"
    assert columns["unparsed"] == "VARCHAR"


def test_geojson_property_columns_mixed_kinds_fall_back_to_varchar(tmp_path):
    path = _write_geojson(
        tmp_path,
        [{"height_m": 12.5}, {"height_m": "unknown"}],
    )

    columns = dict(source_schema._geojson_property_columns(str(path)))

    assert columns["height_m"] == "VARCHAR"


def test_geojson_property_columns_scan_every_feature(tmp_path):
    # A property absent (or null) in the first feature still sniffs, from the
    # values in the features where it does appear.
    path = _write_geojson(
        tmp_path,
        [{"name": "a", "surface_width_m": None}, {"name": "b", "surface_width_m": 2.5}],
    )

    columns = dict(source_schema._geojson_property_columns(str(path)))

    assert columns["surface_width_m"] == "DOUBLE"


# init_column_metadata: the produced TSV carries the true type, whichever
# sniff path ran (ST_Read DESCRIBE when spatial is available, the value-based
# fallback when it is not).


def test_init_column_metadata_geojson_types_match_the_data(tmp_path):
    path = _write_geojson(
        tmp_path,
        [
            {"street_type": "residential", "width_m": None},
            {"street_type": "service", "width_m": 3.5},
        ],
    )

    file_metadata = pd.DataFrame(
        [
            {
                "schema": "staging",
                "dataset": "roads",
                "file_regex": str(path),
                "delim": "c",
            }
        ]
    )

    df = source_schema.init_column_metadata(file_metadata=file_metadata)
    by_name = {row["column_name"]: row["data_type"] for _, row in df.iterrows()}

    assert by_name["street_type"] == "VARCHAR"
    assert by_name["width_m"] == "DOUBLE"


# Staging lineage: the declared type crosses the wildcard bridge and the cast
# that sets the staged column's type is observable.


def _build_staging(tmp_path, column_rows):
    path = _write_geojson(tmp_path, [{"street_type": "residential", "width_m": 3.5}])

    pd.DataFrame(
        [
            {
                "schema": "staging",
                "dataset": "roads",
                "file_regex": str(path),
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
            extensions=SPATIAL,
        )
    )

    return staging_schema.Staging(
        source=source,
        settings=staging_schema.StagingSettings(name="staging", extensions=SPATIAL),
    )


def _metadata_row(**overrides):
    row = {
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
    row.update(overrides)
    return row


def test_staging_column_type_and_its_lineage_origin(tmp_path):
    staging = _build_staging(
        tmp_path,
        [
            _metadata_row(column_name="street_type", ordinal_position=0),
            _metadata_row(
                column_name="width_m",
                var_type="numeric",
                data_type="DOUBLE",
                ordinal_position=1,
            ),
        ],
    )

    column = staging.tables["roads"].columns["width_m"]

    # The staged column carries the declared type -- what the staging CAST produces.
    assert column.data_type == lineage_values.Datatype("DOUBLE")

    nodes = list(column.root_graph().rx_graph.nodes())
    by_type = {}
    for node in nodes:
        by_type.setdefault(node.get("type", ""), []).append(node)

    # The ColumnMetadata node records the declared type at its origin...
    metadata_nodes = by_type["<class 'tyr.lineage.schema.source.ColumnMetadata'>"]
    assert metadata_nodes[0]["data_type"] == "DOUBLE"
    # ...the wildcard bridge passes it through (not dropped as "")...
    bridge_nodes = by_type[
        "<class 'tyr.lineage.functions.utility.SourceWildToStagingColumn'>"
    ]
    assert bridge_nodes[0]["data_type"] == "DOUBLE"
    # ...and the staging cast is recorded as the place the staged type is set.
    cast_nodes = by_type["<class 'tyr.lineage.functions.data_type.TryCast'>"]
    assert cast_nodes[0]["data_type"] == "DOUBLE"


def test_staging_materialises_with_the_propagated_type(tmp_path):
    staging = _build_staging(
        tmp_path,
        [
            _metadata_row(column_name="street_type", ordinal_position=0),
            _metadata_row(
                column_name="width_m",
                var_type="numeric",
                data_type="DOUBLE",
                ordinal_position=1,
            ),
        ],
    )

    conn = tyr.database.connections.Connection(
        name="test", syntax="duckdb", database=":memory:"
    )
    try:
        conn.execute(staging.source.settings.sql)
        conn.execute(staging.settings.sql)
        tyr.database.core.create_tables(staging.source, conn)
        tyr.database.core.create_tables(staging, conn)

        staged_types = dict(
            zip(
                conn.execute("PRAGMA table_info('staging.roads')").df()["name"],
                conn.execute("PRAGMA table_info('staging.roads')").df()["type"],
            )
        )
        assert staged_types["width_m"] == "DOUBLE"
        assert conn.execute('SELECT "width_m" FROM staging.roads').df()[
            "width_m"
        ].tolist() == [3.5]
    finally:
        conn.close()
