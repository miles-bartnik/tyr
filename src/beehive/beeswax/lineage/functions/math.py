from ...lineage import core as lineage
from ...lineage import values as lineage_values
from ...lineage import expressions as lineage_expressions
from typing import List as TypingList, Any
import re
import pandas as pd


class Divide(lineage._Function):
    def __init__(
        self,
        left,
        right,
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
            unit=lineage.units.core.Unit(left.unit.name + right.unit.reciprocal().name),
        )


class Multiply(lineage._Function):
    def __init__(
        self,
        left,
        right,
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
            unit=lineage.units.core.Unit(left.unit.name + right.unit.name),
        )


class Add(lineage._Function):
    def __init__(
        self,
        left,
        right,
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

        if (left.unit.sub_units.empty) and (right.unit.sub_units.empty):
            unit = lineage.units.core.Unit()
        elif left.unit.sub_units.empty:
            raise AttributeError(
                rf"Units are not the same: left: {left.unit.name}, right: {right.unit.name}"
            )
        elif right.unit.sub_units.empty:
            raise AttributeError(
                rf"Units are not the same: left: {left.unit.name}, right: {right.unit.name}"
            )
        elif left.unit.sub_units.sort_values(
            ["sub_unit_exponent", "sub_unit_symbol"]
        ).equals(
            right.unit.sub_units.sort_values(["sub_unit_exponent", "sub_unit_symbol"])
        ):
            unit = left.unit
        else:
            raise AttributeError(
                rf"Units are not the same: left: {left.unit.name}, right: {right.unit.name}"
            )

        super().__init__(
            name="ADD",
            args=[left, right],
            data_type=lineage_values.Datatype("FLOAT"),
            unit=unit,
        )


class Subtract(lineage._Function):
    def __init__(
        self,
        left,
        right,
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

        if (left.unit.sub_units.empty) and (right.unit.sub_units.empty):
            unit = lineage.units.core.Unit()
        elif left.unit.sub_units.empty:
            raise AttributeError(
                rf"Units are not the same: left: {left.unit.name}, right: {right.unit.name}"
            )
        elif right.unit.sub_units.empty:
            raise AttributeError(
                rf"Units are not the same: left: {left.unit.name}, right: {right.unit.name}"
            )
        elif left.unit.sub_units.sort_values(
            ["sub_unit_exponent", "sub_unit_symbol"]
        ).equals(
            right.unit.sub_units.sort_values(["sub_unit_exponent", "sub_unit_symbol"])
        ):
            unit = left.unit
        else:
            raise AttributeError(
                rf"Units are not the same: left: {left.unit.name}, right: {right.unit.name}"
            )

        super().__init__(
            name="SUBTRACT",
            args=[left, right],
            data_type=lineage_values.Datatype("FLOAT"),
            unit=unit,
        )


class Exponent(lineage._Function):
    def __init__(
        self,
        source,
        exponent,
    ):
        super().__init__(
            name="POW",
            args=[source, exponent],
            data_type=lineage_values.Datatype("FLOAT"),
            unit=source.unit,
        )


class Sin(lineage._Function):
    def __init__(self, source):
        super().__init__(
            args=[source], name="SIN", data_type=lineage_values.Datatype("FLOAT")
        )


class Cos(lineage._Function):
    def __init__(self, source):
        super().__init__(
            args=[source], name="COS", data_type=lineage_values.Datatype("FLOAT")
        )


class Tan(lineage._Function):
    def __init__(self, source):
        super().__init__(
            args=[source], name="TAN", data_type=lineage_values.Datatype("FLOAT")
        )


class ATan2(lineage._Function):
    def __init__(self, x, y):
        super().__init__(
            args=[x, y], name="ATAN2", data_type=lineage_values.Datatype("FLOAT")
        )


class ASin(lineage._Function):
    def __init__(self, source):
        super().__init__(
            args=[source], name="ASIN", data_type=lineage_values.Datatype("FLOAT")
        )


class Radians(lineage._Function):
    def __init__(self, source):
        super().__init__(
            args=[source], name="RADIANS", data_type=lineage_values.Datatype("FLOAT")
        )


class Degrees(lineage._Function):
    def __init__(self, source):
        super().__init__(
            args=[source], name="DEGREES", data_type=lineage_values.Datatype("FLOAT")
        )


class Round(lineage._Function):
    def __init__(self, source, precision):
        super().__init__(
            args=[source, precision],
            name="ROUND",
            data_type=source.data_type,
            unit=source.unit,
        )


class Ceiling(lineage._Function):
    def __init__(
        self,
        source,
    ):
        super().__init__(
            args=[source],
            name="CEIL",
            data_type=lineage_values.Datatype("INTEGER"),
            var_type="numeric",
        )


class Floor(lineage._Function):
    def __init__(
        self,
        source,
    ):
        super().__init__(
            args=[source],
            name="FLOOR",
            data_type=lineage_values.Datatype("INTEGER"),
            var_type="numeric",
        )


class Log10(lineage._Function):
    def __init__(
        self,
        source,
    ):
        super().__init__(
            args=[source],
            name="LOG10",
            data_type=lineage_values.Datatype("FLOAT"),
            var_type="numeric",
        )


class AbsoluteValue(lineage._Function):
    def __init__(
        self,
        source,
    ):
        super().__init__(
            args=[source],
            name="ABS",
            data_type=source.data_type,
            var_type="numeric",
        )


class Pi(lineage._Function):
    def __init__(self):
        super().__init__(
            args=[],
            name="PI",
            data_type=lineage_values.Datatype("FLOAT"),
            var_type="numeric",
        )


class Mod(lineage._Function):
    def __init__(self, x, y):
        super().__init__(
            args=[x, y],
            name="FMOD",
            data_type=lineage_values.Datatype("DOUBLE"),
            var_type="numeric",
        )
