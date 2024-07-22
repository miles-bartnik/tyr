from ...lineage import core as lineage
from ...lineage import values as lineage_values
from ...lineage import expressions as lineage_expressions
from typing import List as TypingList, Any
import re
import pandas as pd


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
