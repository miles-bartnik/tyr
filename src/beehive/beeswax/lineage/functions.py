from ..lineage import core as lineage
from ..lineage import values as lineage_values
from ..lineage import expressions as lineage_expressions
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


class EpochMSToTimestamp(lineage._Function):
    def __init__(
        self,
        source,
    ):
        super().__init__(name="EPOCH_MS", args=[source], var_type=source.var_type)


class EpochToTimestamp(lineage._Function):
    def __init__(
        self,
        source,
    ):
        super().__init__(name="EPOCH", args=[source], var_type=source.var_type)


class TimestampToEpochMS(lineage._Function):
    def __init__(
        self,
        source,
    ):
        super().__init__(name="EPOCH_MS", args=[source], var_type=source.var_type)


class StringToTimestamp(lineage._Function):
    def __init__(
        self,
        source,
        timestamp_format,
    ):
        super().__init__(
            name="STRPTIME",
            args=[source, timestamp_format],
            var_type=source.var_type,
            data_type=lineage_values.Datatype("TIMESTAMP"),
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


class RegExpExtract(lineage._Function):
    def __init__(
        self,
        source,
        regex,
        match_number=None,
    ):
        self.source = source
        self.regex = regex

        if match_number:
            self.match_number = match_number
        else:
            match_number = lineage_values.Integer(1)

        super().__init__(
            name="REGEXP_EXTRACT",
            args=[source, regex, match_number],
            var_type=source.var_type,
            data_type=source.data_type,
        )


class RegExpExtractAll(lineage._Function):
    def __init__(
        self,
        source,
        regex,
    ):
        self.source = source
        self.regex = regex

        super().__init__(
            name="REGEXP_EXTRACT_ALL",
            args=[source, regex],
            var_type=source.var_type,
            data_type=lineage_values.Datatype(source.data_type.value + "[]"),
        )


class RegExpContains(lineage._Function):
    def __init__(
        self,
        source,
        regex,
    ):
        self.source = source
        self.regex = regex

        super().__init__(
            name="REGEXP_MATCHES",
            args=[source, regex],
            var_type="categorical",
            data_type=lineage_values.Datatype("BOOLEAN"),
        )


class RegExpMatch(lineage._Function):
    def __init__(
        self,
        source,
        regex,
    ):
        self.source = source
        self.regex = regex

        super().__init__(
            name="REGEXP_FULL_MATCH",
            args=[source, regex],
            var_type="categorical",
            data_type=lineage_values.Datatype("BOOLEAN"),
        )


class GeoCoordinate(lineage._Function):
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


class Lag(lineage._Function):
    def __init__(
        self,
        source,
        partition_by: lineage.PartitionBy = lineage.PartitionBy(lineage.ColumnList([])),
        order_by: lineage.OrderBy = lineage.OrderBy(columns=lineage.ColumnList([])),
    ):
        self.source = source

        super().__init__(
            name="LAG",
            args=[source],
            partition_by=partition_by,
            order_by=order_by,
            var_type=source.var_type,
            data_type=source.data_type,
            unit=source.unit,
        )


class Lead(lineage._Function):
    def __init__(
        self,
        source,
        partition_by: lineage.PartitionBy = lineage.PartitionBy(lineage.ColumnList([])),
        order_by: lineage.OrderBy = lineage.OrderBy(columns=lineage.ColumnList([])),
    ):
        self.source = source

        super().__init__(
            name="LEAD",
            args=[source],
            partition_by=partition_by,
            order_by=order_by,
            var_type=source.var_type,
            data_type=source.data_type,
            unit=source.unit,
        )


class Rank(lineage._Function):
    def __init__(
        self,
        source,
        partition_by: lineage.PartitionBy = lineage.PartitionBy(lineage.ColumnList([])),
        order_by: lineage.OrderBy = lineage.OrderBy(columns=lineage.ColumnList([])),
    ):
        self.source = source

        super().__init__(
            name="RANK",
            args=[source],
            partition_by=partition_by,
            order_by=order_by,
            var_type=source.var_type,
            data_type=source.data_type,
        )


class RowNumber(lineage._Function):
    def __init__(
        self,
        partition_by: lineage.PartitionBy = lineage.PartitionBy(lineage.ColumnList([])),
        order_by: lineage.OrderBy = lineage.OrderBy(columns=lineage.ColumnList([])),
    ):
        self.source = None

        super().__init__(
            name="ROW_NUMBER",
            args=[],
            partition_by=partition_by,
            order_by=order_by,
            var_type=None,
            data_type=lineage_values.Datatype("INTEGER"),
        )


class Unnest(lineage._Function):
    def __init__(
        self,
        source,
    ):
        self.source = source

        if source.data_type:
            data_type = lineage_values.Datatype(source.data_type.value.strip("[]"))
        else:
            data_type = None

        super().__init__(
            name="UNNEST",
            args=[source],
            var_type=source.var_type,
            data_type=data_type,
        )


class Range(lineage._Function):
    def __init__(
        self,
        start,
        end,
        interval,
    ):
        self.start = start
        self.end = end
        self.interval = interval

        super().__init__(
            name="RANGE",
            args=[start, end, interval],
            var_type=start.var_type,
            data_type=start.data_type,
        )


class TruncateTimestamp(lineage._Function):
    def __init__(
        self,
        source,
        granularity: lineage_values.Varchar,
    ):
        self.source = source
        self.granularity = granularity

        super().__init__(
            args=[granularity, source],
            name="DATE_TRUNC",
            data_type=lineage_values.Datatype("TIMESTAMP"),
        )


class DatePart(lineage._Function):
    def __init__(self, source, part: lineage_values.Varchar):
        super().__init__(
            args=[part, source],
            name="DATE_PART",
            data_type=lineage_values.Datatype("INTEGER"),
        )


class DateDiff(lineage._Function):
    def __init__(self, start, end, unit):
        unit = lineage_values.Varchar(unit.sub_units.iloc[0].base_unit_name.upper())

        super().__init__(
            args=[unit, start, end],
            name="DATE_DIFF",
            data_type=lineage_values.Datatype("INTERVAL"),
        )


class DateBin(lineage._Function):
    def __init__(self, source, interval, offset=None):
        super().__init__(
            args=[arg for arg in [interval, source, offset] if arg],
            name="TIME_BUCKET",
            data_type=lineage_values.Datatype("TIMESTAMP"),
        )


class ConcatenateStrings(lineage._Function):
    def __init__(self, strings: TypingList[Any], join_string=None):
        if join_string:
            args = [
                x for y in [[string, join_string] for string in strings[:-1]] for x in y
            ] + [strings[:-1]]
        else:
            args = strings

        super().__init__(args=args, name="CONCAT")


class Sum(lineage._Function):
    def __init__(self, source):
        super().__init__(args=[source], name="SUM")


class Count(lineage._Function):
    def __init__(
        self,
        source,
        partition_by: lineage.PartitionBy = lineage.PartitionBy(lineage.ColumnList([])),
        order_by: lineage.OrderBy = lineage.OrderBy(columns=lineage.ColumnList([])),
    ):
        super().__init__(
            args=[source], partition_by=partition_by, order_by=order_by, name="COUNT"
        )


class Error(lineage._Function):
    def __init__(self, message: lineage_values.Varchar):
        super().__init__(args=[message], name="ERROR")


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


class GeoAsWKT(lineage._Function):
    def __init__(self, source):
        super().__init__(
            args=[source],
            name="ST_ASTEXT",
            data_type=lineage_values.Datatype("VARCHAR"),
        )


class GeoAsGeoJSON(lineage._Function):
    def __init__(self, source):
        super().__init__(
            args=[source],
            name="ST_ASGEOJSON",
            data_type=lineage_values.Datatype("JSON"),
        )


class ToInterval(lineage._Function):
    def __init__(self, source, unit: lineage.units.core.Unit):
        super().__init__(
            args=[source, unit],
            name="INTERVAL",
            data_type=lineage_values.Datatype("INTERVAL"),
            var_type=source.var_type,
        )


class Length(lineage._Function):
    def __init__(self, source):
        super().__init__(
            args=[source],
            name="LENGTH",
            data_type=lineage_values.Datatype("INTEGER"),
            var_type="numeric",
        )


class QuantileCont(lineage._Function):
    def __init__(
        self,
        source,
    ):
        super().__init__(
            args=[source],
            name="LENGTH",
            data_type=lineage_values.Datatype("INTEGER"),
            var_type="numeric",
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


class DateAdd(lineage._Function):
    def __init__(self, source, interval):
        super().__init__(
            args=[source, interval],
            name="DATE_ADD",
            data_type=source.data_type,
            var_type="timestamp",
        )


class List(lineage._Function):
    def __init__(self, values):
        if len(list(set([value.data_type.name for value in values]))) > 1:
            raise ValueError("Mixed data_types provided in values")

        super().__init__(
            args=values,
            name="LIST",
            data_type=values[0].data_type,
            var_type=values[0].var_type,
        )


class ListExtract(lineage._Function):
    def __init__(self, source, elements):
        super().__init__(
            args=[source, elements],
            name="LIST_EXTRACT",
            data_type=lineage_values.Datatype(source.data_type.value.strip("[]")),
            var_type=source.var_type,
        )


class JSONExtract(lineage._Function):
    def __init__(self, source, key):
        super().__init__(
            args=[source, key],
            name="JSON_EXTRACT",
            data_type=lineage_values.Datatype(source.data_type.value.strip("[]")),
            var_type=source.var_type,
        )
