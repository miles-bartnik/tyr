from ...lineage import core as lineage
from ...lineage import values as lineage_values
from ...lineage import expressions as lineage_expressions
from ..units.core import Unit


class Cast(lineage._Function):
    def __init__(
        self,
        source,
        data_type: lineage_values.Datatype,
    ):
        super().__init__(
            name="CAST",
            args=[lineage_expressions.As(source, data_type)],
            data_type=data_type,
            var_type=source.var_type,
            unit=source.unit,
        )


class TryCast(lineage._Function):
    def __init__(
        self,
        source,
        data_type,
    ):
        super().__init__(
            name="TRY_CAST",
            args=[lineage_expressions.As(source, data_type)],
            data_type=data_type,
            var_type=source.var_type,
            unit=source.unit,
        )


class IntegerToHex(lineage._Function):
    def __init__(self, source):
        if source.data_type.value != "INTEGER":
            raise ValueError("source must be INTEGER")

        super().__init__(
            args=[source],
            name="HEX",
            data_type=lineage_values.Datatype("VARCHAR"),
        )
