from ...lineage import core as lineage
from ...lineage import values as lineage_values


class Coordinate(lineage._Function):
    def __init__(
        self,
        lat,
        long,
        macro_group: str = "",
    ):
        super().__init__(
            name="ST_POINT",
            args=[long, lat],
            data_type=lineage_values.Datatype("GEOMETRY"),
            var_type="geometry",
            macro_group=macro_group,
        )


class Contains(lineage._Function):
    def __init__(
        self,
        source,
        bounds,
        macro_group: str = "",
    ):
        super().__init__(
            name="ST_CONTAINS",
            args=[bounds, source],
            data_type=lineage_values.Datatype("BOOLEAN"),
            var_type="categorical",
            macro_group=macro_group,
        )


class AsWKT(lineage._Function):
    def __init__(self, source, macro_group: str = ""):
        super().__init__(
            args=[source],
            name="ST_ASTEXT",
            data_type=lineage_values.Datatype("VARCHAR"),
            macro_group=macro_group,
        )


class AsGeoJSON(lineage._Function):
    def __init__(self, source, macro_group: str = ""):
        super().__init__(
            args=[source],
            name="ST_ASGEOJSON",
            data_type=lineage_values.Datatype("JSON"),
            macro_group=macro_group,
        )


class FromWKT(lineage._Function):
    def __init__(self, source, macro_group: str = ""):
        super().__init__(
            args=[source],
            name="ST_GEOMFROMTEXT",
            data_type=lineage_values.Datatype("GEOMETRY"),
            macro_group=macro_group,
        )


class H3LatLongToCell(lineage._Function):
    def __init__(self, lat, long, precision, macro_group: str = ""):
        super().__init__(
            args=[lat, long, precision],
            name="H3_LATLNG_TO_CELL",
            data_type=lineage_values.Datatype("VARCHAR"),
            macro_group=macro_group,
        )


class H3CellToBoundaryWKT(lineage._Function):
    def __init__(self, cell, macro_group: str = ""):
        super().__init__(
            args=[cell],
            name="H3_CELL_TO_BOUNDARY_WKT",
            data_type=lineage_values.Datatype("GEOMETRY"),
            macro_group=macro_group,
        )
