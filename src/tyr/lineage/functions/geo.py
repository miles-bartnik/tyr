from ...lineage import core as lineage
from ...lineage import values as lineage_values


class Coordinate(lineage._Function):
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
    def __init__(self, source):
        super().__init__(
            args=[source],
            name="ST_ASTEXT",
            data_type=lineage_values.Datatype("VARCHAR"),
        )


class AsGeoJSON(lineage._Function):
    def __init__(self, source):
        super().__init__(
            args=[source],
            name="ST_ASGEOJSON",
            data_type=lineage_values.Datatype("JSON"),
        )


class FromWKT(lineage._Function):
    def __init__(self, source):
        super().__init__(
            args=[source],
            name="ST_GEOMFROMTEXT",
            data_type=lineage_values.Datatype("GEOMETRY"),
        )


class H3LatLongToCell(lineage._Function):
    def __init__(self, lat, long, precision):
        super().__init__(
            args=[lat, long, precision],
            name="H3_LATLNG_TO_CELL",
            data_type=lineage_values.Datatype("VARCHAR"),
        )


class H3CellToLatLong(lineage._Function):
    def __init__(self, source):
        super().__init__(
            args=[source],
            name="H3_CELL_TO_LATLNG",
            data_type=lineage_values.Datatype("DOUBLE[]"),
        )


class H3CellToBoundaryWKT(lineage._Function):
    def __init__(self, cell):
        super().__init__(
            args=[cell],
            name="H3_CELL_TO_BOUNDARY_WKT",
            data_type=lineage_values.Datatype("GEOMETRY"),
        )
