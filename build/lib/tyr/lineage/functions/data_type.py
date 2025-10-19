from ...lineage import core as lineage
from ...lineage import values as lineage_values
from ...lineage import expressions as lineage_expressions


class Cast(lineage._Function):
    def __init__(
        self,
        source,
        data_type: lineage_values.Datatype,
        macro_group: str = "",
    ):
        super().__init__(
            name="CAST",
            args=[lineage_expressions.As(source, data_type)],
            data_type=data_type,
            var_type=source.var_type,
            unit=source.unit,
            macro_group=macro_group,
        )


class TryCast(lineage._Function):
    def __init__(
        self,
        source,
        data_type,
        macro_group: str = "",
    ):
        super().__init__(
            name="TRY_CAST",
            args=[lineage_expressions.As(source, data_type)],
            data_type=data_type,
            var_type=source.var_type,
            unit=source.unit,
            macro_group=macro_group,
        )


class ToInterval(lineage._Function):
    def __init__(self, source, unit: lineage.units.core.Unit, macro_group: str = ""):
        super().__init__(
            args=[source, unit],
            name="INTERVAL",
            data_type=lineage_values.Datatype("INTERVAL"),
            var_type=source.var_type,
            macro_group=macro_group,
        )


class IntegerToHex(lineage._Function):
    def __init__(self, source, macro_group: str = ""):
        if source.data_type.value != "INTEGER":
            raise ValueError("source must be INTEGER")

        super().__init__(
            args=[source],
            name="HEX",
            data_type=lineage_values.Datatype("VARCHAR"),
            macro_group=macro_group,
        )
