from ...lineage import core as lineage
from ...lineage import values as lineage_values


class Coordinate(lineage._Function):
    """
    Construct a point geometry from x/y (spatial ST_Point).

    DuckDB: https://duckdb.org/docs/stable/core_extensions/spatial/functions#st_point
    """
    def __init__(
        self,
        lat,
        long,
    ):
        super().__init__(
            name="ST_POINT",
            args=[long, lat],
            data_type=lineage_values.Datatype("GEOMETRY"),
            var_type="geometry",
        )


class Contains(lineage._Function):
    """
    Whether geometry A contains geometry B (spatial ST_Contains).

    DuckDB: https://duckdb.org/docs/stable/core_extensions/spatial/functions#st_contains
    """
    def __init__(
        self,
        source,
        bounds,
    ):
        super().__init__(
            name="ST_CONTAINS",
            args=[bounds, source],
            data_type=lineage_values.Datatype("BOOLEAN"),
            var_type="categorical",
        )


class AsWKT(lineage._Function):
    """
    Well-Known Text of a geometry (spatial ST_AsText).

    DuckDB: https://duckdb.org/docs/stable/core_extensions/spatial/functions#st_astext
    """
    def __init__(self, source):
        super().__init__(
            args=[source],
            name="ST_ASTEXT",
            data_type=lineage_values.Datatype("VARCHAR"),
        )


class AsGeoJSON(lineage._Function):
    """
    GeoJSON text of a geometry (spatial ST_AsGeoJSON).

    DuckDB: https://duckdb.org/docs/stable/core_extensions/spatial/functions#st_asgeojson
    """
    def __init__(self, source):
        super().__init__(
            args=[source],
            name="ST_ASGEOJSON",
            data_type=lineage_values.Datatype("JSON"),
        )


class FromWKT(lineage._Function):
    """
    Parse a geometry from Well-Known Text (spatial ST_GeomFromText).

    DuckDB: https://duckdb.org/docs/stable/core_extensions/spatial/functions#st_geomfromtext
    """
    def __init__(self, source):
        super().__init__(
            args=[source],
            name="ST_GEOMFROMTEXT",
            data_type=lineage_values.Datatype("GEOMETRY"),
        )


class H3LatLongToCell(lineage._Function):
    """
    H3 cell index for a lat/long at a resolution (h3 extension h3_latlng_to_cell).

    DuckDB: https://duckdb.org/community_extensions/extensions/h3.html#added-functions
    """
    def __init__(self, lat, long, precision):
        super().__init__(
            args=[lat, long, precision],
            name="H3_LATLNG_TO_CELL_STRING",
            data_type=lineage_values.Datatype("VARCHAR"),
        )


class H3CellToLatLong(lineage._Function):
    """
    Latitude/longitude centre of an H3 cell (h3 extension h3_cell_to_latlng).

    DuckDB: https://duckdb.org/community_extensions/extensions/h3.html#added-functions
    """
    def __init__(self, source):
        super().__init__(
            args=[source],
            name="H3_CELL_TO_LATLNG",
            data_type=lineage_values.Datatype("DOUBLE[]"),
        )


class H3CellToBoundaryWKT(lineage._Function):
    """
    WKT polygon boundary of an H3 cell (h3 extension h3_cell_to_boundary_wkt).

    DuckDB: https://duckdb.org/community_extensions/extensions/h3.html#added-functions
    """
    def __init__(self, cell):
        super().__init__(
            args=[cell],
            name="H3_CELL_TO_BOUNDARY_WKT",
            data_type=lineage_values.Datatype("GEOMETRY"),
        )
