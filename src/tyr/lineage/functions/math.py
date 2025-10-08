import warnings

from ...lineage import core as lineage
from ...lineage import values as lineage_values


class Divide(lineage._Function):
    def __init__(
        self,
        left,
        right,
        macro_group: str = "",
    ):
        if any(
            [left.data_type.value == "INTERVAL", right.data_type.value == "INTERVAL"]
        ):
            data_type = lineage_values.Datatype("INTERVAL")
        else:
            data_type = lineage_values.Datatype("FLOAT")

        super().__init__(
            name="DIVIDE",
            args=[left, right],
            data_type=data_type,
            unit=lineage.units.core.divide(left.unit, right.unit),
            macro_group=macro_group,
        )


class Multiply(lineage._Function):
    def __init__(
        self,
        left,
        right,
        macro_group: str = "",
    ):
        if any(
            [left.data_type.value == "INTERVAL", right.data_type.value == "INTERVAL"]
        ):
            data_type = lineage_values.Datatype("INTERVAL")
        elif all(
            [left.data_type.value == "INTEGER", right.data_type.value == "INTEGER"]
        ):
            data_type = lineage_values.Datatype("INTEGER")
        else:
            data_type = lineage_values.Datatype("FLOAT")

        super().__init__(
            name="MULTIPLY",
            args=[left, right],
            data_type=data_type,
            unit=lineage.units.core.multiply(left.unit, right.unit),
            macro_group=macro_group,
        )


class Add(lineage._Function):
    def __init__(
        self,
        left,
        right,
        macro_group: str = "",
    ):
        if all(
            [left.data_type.value == "INTERVAL", right.data_type.value == "INTERVAL"]
        ):
            data_type = lineage_values.Datatype("INTERVAL")
        elif all(
            [left.data_type.value == "INTEGER", right.data_type.value == "INTEGER"]
        ):
            data_type = lineage_values.Datatype("INTEGER")
        else:
            data_type = lineage_values.Datatype("FLOAT")

        super().__init__(
            name="ADD",
            args=[left, right],
            data_type=lineage_values.Datatype("FLOAT"),
            unit=lineage.units.core.add_subtract(left.unit, right.unit),
            macro_group=macro_group,
        )


class Subtract(lineage._Function):
    def __init__(
        self,
        left,
        right,
        macro_group: str = "",
    ):
        if all(
            [left.data_type.value == "INTERVAL", right.data_type.value == "INTERVAL"]
        ):
            data_type = lineage_values.Datatype("INTERVAL")
        elif all(
            [left.data_type.value == "INTEGER", right.data_type.value == "INTEGER"]
        ):
            data_type = lineage_values.Datatype("INTEGER")
        else:
            data_type = lineage_values.Datatype("FLOAT")

        super().__init__(
            name="SUBTRACT",
            args=[left, right],
            data_type=lineage_values.Datatype("FLOAT"),
            unit=lineage.units.core.add_subtract(left.unit, right.unit),
            macro_group=macro_group,
        )


class Exponent(lineage._Function):
    def __init__(
        self,
        source,
        exponent,
        macro_group: str = "",
    ):
        if any(
            [
                isinstance(exponent, lineage_values.Integer),
                isinstance(exponent, lineage_values.Float),
            ]
        ):
            super().__init__(
                name="POW",
                args=[source, exponent],
                data_type=lineage_values.Datatype("FLOAT"),
                unit=lineage.units.core.exponent(source.unit, exponent.value),
                macro_group=macro_group,
            )

        else:
            warnings.warn(
                "Variable exponents applied to a vector will not produce a consistent unit. Defaulting to source unit"
            )

            super().__init__(
                name="POW",
                args=[source, exponent],
                data_type=lineage_values.Datatype("FLOAT"),
                unit=source.unit,
                macro_group=macro_group,
            )


class Sin(lineage._Function):
    def __init__(self, source, macro_group: str = ""):
        super().__init__(
            args=[source],
            name="SIN",
            data_type=lineage_values.Datatype("FLOAT"),
            macro_group=macro_group,
        )


class Cos(lineage._Function):
    def __init__(self, source, macro_group: str = ""):
        super().__init__(
            args=[source],
            name="COS",
            data_type=lineage_values.Datatype("FLOAT"),
            macro_group=macro_group,
        )


class Tan(lineage._Function):
    def __init__(self, source, macro_group: str = ""):
        super().__init__(
            args=[source],
            name="TAN",
            data_type=lineage_values.Datatype("FLOAT"),
            macro_group=macro_group,
        )


class ATan2(lineage._Function):
    def __init__(self, x, y, macro_group: str = ""):
        super().__init__(
            args=[x, y],
            name="ATAN2",
            data_type=lineage_values.Datatype("FLOAT"),
            macro_group=macro_group,
        )


class ASin(lineage._Function):
    def __init__(self, source, macro_group: str = ""):
        super().__init__(
            args=[source],
            name="ASIN",
            data_type=lineage_values.Datatype("FLOAT"),
            macro_group=macro_group,
        )


class Radians(lineage._Function):
    def __init__(self, source, macro_group: str = ""):
        super().__init__(
            args=[source],
            name="RADIANS",
            data_type=lineage_values.Datatype("FLOAT"),
            macro_group=macro_group,
        )


class Degrees(lineage._Function):
    def __init__(self, source, macro_group: str = ""):
        super().__init__(
            args=[source],
            name="DEGREES",
            data_type=lineage_values.Datatype("FLOAT"),
            macro_group=macro_group,
        )


class Round(lineage._Function):
    def __init__(self, source, precision, macro_group: str = ""):
        super().__init__(
            args=[source, precision],
            name="ROUND",
            data_type=source.data_type,
            unit=source.unit,
            macro_group=macro_group,
        )


class Ceiling(lineage._Function):
    def __init__(
        self,
        source,
        macro_group: str = "",
    ):
        super().__init__(
            args=[source],
            name="CEIL",
            data_type=lineage_values.Datatype("INTEGER"),
            var_type="numeric",
            macro_group=macro_group,
        )


class Floor(lineage._Function):
    def __init__(
        self,
        source,
        macro_group: str = "",
    ):
        super().__init__(
            args=[source],
            name="FLOOR",
            data_type=lineage_values.Datatype("INTEGER"),
            var_type="numeric",
            macro_group=macro_group,
            unit=source.unit,
        )


class Log10(lineage._Function):
    def __init__(
        self,
        source,
        macro_group: str = "",
    ):
        super().__init__(
            args=[source],
            name="LOG10",
            data_type=lineage_values.Datatype("FLOAT"),
            var_type="numeric",
            macro_group=macro_group,
        )


class AbsoluteValue(lineage._Function):
    def __init__(
        self,
        source,
        macro_group: str = "",
    ):
        super().__init__(
            args=[source],
            name="ABS",
            data_type=source.data_type,
            var_type="numeric",
            macro_group=macro_group,
        )


class Pi(lineage._Function):
    def __init__(self, macro_group: str = ""):
        super().__init__(
            args=[],
            name="PI",
            data_type=lineage_values.Datatype("FLOAT"),
            var_type="numeric",
            macro_group=macro_group,
        )


class Mod(lineage._Function):
    def __init__(self, x, y, macro_group: str = ""):
        super().__init__(
            args=[x, y],
            name="FMOD",
            data_type=lineage_values.Datatype("DOUBLE"),
            var_type="numeric",
            macro_group=macro_group,
        )
