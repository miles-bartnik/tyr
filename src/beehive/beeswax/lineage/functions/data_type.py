from ...lineage import core as lineage
from ...lineage import values as lineage_values
from ...lineage import expressions as lineage_expressions
from typing import List as TypingList, Any
import re
import pandas as pd


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


class ToInterval(lineage._Function):
    def __init__(self, source, unit: lineage.units.core.Unit):
        super().__init__(
            args=[source, unit],
            name="INTERVAL",
            data_type=lineage_values.Datatype("INTERVAL"),
            var_type=source.var_type,
        )


class IntegerToHex(lineage._Function):
    def __init__(self, source):
        if source.data_type.value is not "INTEGER":
            raise ValueError("source must be INTEGER")

        super().__init__(
            args=[source],
            name="HEX",
            data_type=lineage_values.Datatype("VARCHAR"),
        )
