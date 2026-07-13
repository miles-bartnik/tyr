import warnings

from ...lineage import core as lineage
from ...lineage import values as lineage_values
from ...lineage import units


class Divide(lineage._Function):
    """
    Divide two numbers (SQL / operator).

    DuckDB: https://duckdb.org/docs/stable/sql/functions/numeric
    """
    def __init__(
        self,
        left,
        right,
    ):
        if any(
            [
                left.data_type == lineage_values.Datatype("INTERVAL"),
                right.data_type == lineage_values.Datatype("INTERVAL"),
            ]
        ):
            data_type = lineage_values.Datatype("INTERVAL")
        else:
            data_type = lineage_values.Datatype("FLOAT")

        super().__init__(
            name="DIVIDE",
            args=[left, right],
            data_type=data_type,
            unit=units.core.divide(left.unit, right.unit),
        )


class Multiply(lineage._Function):
    """
    Multiply two numbers (SQL * operator).

    DuckDB: https://duckdb.org/docs/stable/sql/functions/numeric
    """
    def __init__(
        self,
        left,
        right,
    ):
        if any(
            [
                left.data_type == lineage_values.Datatype("INTERVAL"),
                right.data_type == lineage_values.Datatype("INTERVAL"),
            ]
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
            unit=units.core.multiply(left.unit, right.unit),
        )


class Add(lineage._Function):
    """
    Add two numbers (SQL + operator).

    DuckDB: https://duckdb.org/docs/stable/sql/functions/numeric
    """
    def __init__(
        self,
        left,
        right,
    ):
        if all(
            [
                left.data_type == lineage_values.Datatype("INTERVAL"),
                right.data_type == lineage_values.Datatype("INTERVAL"),
            ]
        ):
            data_type = lineage_values.Datatype("INTERVAL")
        elif all(
            [
                left.data_type == lineage_values.Datatype("INTERVAL"),
                right.data_type == lineage_values.Datatype("INTERVAL"),
            ]
        ):
            data_type = lineage_values.Datatype("INTEGER")
        else:
            data_type = lineage_values.Datatype("FLOAT")

        super().__init__(
            name="ADD",
            args=[left, right],
            data_type=lineage_values.Datatype("FLOAT"),
            unit=units.core.add_subtract(left.unit, right.unit),
        )


class Subtract(lineage._Function):
    """
    Subtract two numbers (SQL - operator).

    DuckDB: https://duckdb.org/docs/stable/sql/functions/numeric
    """
    def __init__(
        self,
        left,
        right,
    ):
        if all(
            [
                left.data_type == lineage_values.Datatype("INTERVAL"),
                right.data_type == lineage_values.Datatype("INTERVAL"),
            ]
        ):
            data_type = lineage_values.Datatype("INTERVAL")
        elif all(
            [
                left.data_type == lineage_values.Datatype("INTERVAL"),
                right.data_type == lineage_values.Datatype("INTERVAL"),
            ]
        ):
            data_type = lineage_values.Datatype("INTEGER")
        else:
            data_type = lineage_values.Datatype("FLOAT")

        super().__init__(
            name="SUBTRACT",
            args=[left, right],
            data_type=lineage_values.Datatype("FLOAT"),
            unit=units.core.add_subtract(left.unit, right.unit),
        )


class Exponent(lineage._Function):
    """
    Raise a number to a power (SQL pow).

    DuckDB: https://duckdb.org/docs/stable/sql/functions/numeric#powx-y
    """
    def __init__(
        self,
        source,
        exponent,
    ):
        if any(
            [
                isinstance(exponent, lineage_values.Integer),
                isinstance(exponent, lineage_values.FloatingPoint),
            ]
        ):
            super().__init__(
                name="POW",
                args=[source, exponent],
                data_type=lineage_values.Datatype("FLOAT"),
                unit=units.core.exponent(source.unit, exponent.value),
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
            )


class Sin(lineage._Function):
    """
    Sine (SQL sin).

    DuckDB: https://duckdb.org/docs/stable/sql/functions/numeric#sinx
    """
    def __init__(self, source):
        super().__init__(
            args=[source],
            name="SIN",
            data_type=lineage_values.Datatype("FLOAT"),
        )


class Cos(lineage._Function):
    """
    Cosine (SQL cos).

    DuckDB: https://duckdb.org/docs/stable/sql/functions/numeric#cosx
    """
    def __init__(self, source):
        super().__init__(
            args=[source],
            name="COS",
            data_type=lineage_values.Datatype("FLOAT"),
        )


class Tan(lineage._Function):
    """
    Tangent (SQL tan).

    DuckDB: https://duckdb.org/docs/stable/sql/functions/numeric#tanx
    """
    def __init__(self, source):
        super().__init__(
            args=[source],
            name="TAN",
            data_type=lineage_values.Datatype("FLOAT"),
        )


class ATan2(lineage._Function):
    """
    Arc tangent of y/x, using signs to pick the quadrant (SQL atan2).

    DuckDB: https://duckdb.org/docs/stable/sql/functions/numeric#atan2y-x
    """
    def __init__(self, x, y):
        super().__init__(
            args=[x, y],
            name="ATAN2",
            data_type=lineage_values.Datatype("FLOAT"),
        )


class ASin(lineage._Function):
    """
    Arc sine (SQL asin).

    DuckDB: https://duckdb.org/docs/stable/sql/functions/numeric#asinx
    """
    def __init__(self, source):
        super().__init__(
            args=[source],
            name="ASIN",
            data_type=lineage_values.Datatype("FLOAT"),
        )


class Radians(lineage._Function):
    """
    Convert degrees to radians (SQL radians).

    DuckDB: https://duckdb.org/docs/stable/sql/functions/numeric#radiansx
    """
    def __init__(self, source):
        super().__init__(
            args=[source],
            name="RADIANS",
            data_type=lineage_values.Datatype("FLOAT"),
        )


class Degrees(lineage._Function):
    """
    Convert radians to degrees (SQL degrees).

    DuckDB: https://duckdb.org/docs/stable/sql/functions/numeric#degreesx
    """
    def __init__(self, source):
        super().__init__(
            args=[source],
            name="DEGREES",
            data_type=lineage_values.Datatype("FLOAT"),
        )


class Round(lineage._Function):
    """
    Round to a number of decimal places (SQL round).

    DuckDB: https://duckdb.org/docs/stable/sql/functions/numeric#roundv-numeric-s-integer
    """
    def __init__(self, source, precision):
        super().__init__(
            args=[source, precision],
            name="ROUND",
            data_type=source.data_type,
            unit=source.unit,
        )


class Ceiling(lineage._Function):
    """
    Round up to the nearest integer (SQL ceil).

    DuckDB: https://duckdb.org/docs/stable/sql/functions/numeric#ceilx
    """
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
    """
    Round down to the nearest integer (SQL floor).

    DuckDB: https://duckdb.org/docs/stable/sql/functions/numeric#floorx
    """
    def __init__(
        self,
        source,
    ):
        super().__init__(
            args=[source],
            name="FLOOR",
            data_type=lineage_values.Datatype("INTEGER"),
            var_type="numeric",
            unit=source.unit,
        )


class Log10(lineage._Function):
    """
    Base-10 logarithm (SQL log10).

    DuckDB: https://duckdb.org/docs/stable/sql/functions/numeric#log10x
    """
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
    """
    Absolute value (SQL abs).

    DuckDB: https://duckdb.org/docs/stable/sql/functions/numeric#absx
    """
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
    """
    The constant pi (SQL pi).

    DuckDB: https://duckdb.org/docs/stable/sql/functions/numeric#pi
    """
    def __init__(self):
        super().__init__(
            args=[],
            name="PI",
            data_type=lineage_values.Datatype("FLOAT"),
            var_type="numeric",
        )


class Mod(lineage._Function):
    """
    Floating-point remainder of a division (SQL fmod).

    DuckDB: https://duckdb.org/docs/stable/sql/functions/numeric#fmodx-y
    """
    def __init__(self, x, y):
        super().__init__(
            args=[x, y],
            name="FMOD",
            data_type=lineage_values.Datatype("DOUBLE"),
            var_type="numeric",
        )


class Sign(lineage._Function):
    """
    Sign of a number: -1, 0 or 1 (SQL sign).

    DuckDB: https://duckdb.org/docs/stable/sql/functions/numeric#signx
    """
    def __init__(self, source):
        super().__init__(
            args=[source],
            name="SIGN",
            data_type=lineage_values.Datatype("INTEGER"),
            var_type="numeric",
        )
