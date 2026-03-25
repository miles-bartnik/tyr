import math
import re

from .units.core import Unit

from ..lineage import core as lineage
from ..lineage import operators as lineage_operators
import typing
from .core import SUPPORTED_DATA_TYPES
import typing
import ast


class Datatype(lineage._Value):

    """
    Datatype

    :param value: Datatype e.g. INTEGER/VARCHAR/BOOLEAN etc.
    :type value: str
    """

    def __init__(self, value):
        if type(value) is not str:
            print(value)
            raise ValueError("Datatype value must be str")

        self.value_class = None

        DATA_TYPE_TO_VALUE_MAP = {
            "Array": Array,
            "Bitstring": BitString,
            "Blob": Blob,
            "Boolean": Boolean,
            "Date": Date,
            "Enum": Enum,
            "Interval": Interval,
            "JSON": JSON,
            "Map": Map,
            "Integer": Integer,
            "Decimal": FixedPoint,
            "FloatingPoint": FloatingPoint,
            "Struct": Struct,
            "Varchar": Varchar,
            "Time": Time,
            "Timestamp": Timestamp,
            "WildCard": WildCard,
            "Geometry": Geometry,
            "List": List,
        }

        for key in SUPPORTED_DATA_TYPES:
            for data_type in SUPPORTED_DATA_TYPES[key]:
                if re.search(data_type["data_type_regex"], value):
                    self.value_class_kwargs = re.search(
                        data_type["data_type_regex"], value
                    ).groupdict() | {"unit": data_type["unit"]}

                    self.value_class = DATA_TYPE_TO_VALUE_MAP[data_type["value_class"]]

                    self.value_class_kwargs.pop("data_type")

                    break
                else:
                    pass

        if not self.value_class:
            raise ValueError(rf"data_type {value} not supported")

        super().__init__(value=value, data_type="DATATYPE")

    def to_value(self, value, unit=None):
        # INTERVAL causing weirdness on account of timedelta not having unit attached. Please fix

        if self.value_class_kwargs["unit"]:
            if unit:
                unit = unit
            else:
                unit = Unit()

            value_class_kwargs = self.value_class_kwargs
            value_class_kwargs["unit"] = unit
            value_class_kwargs["value"] = value
        else:
            value_class_kwargs = self.value_class_kwargs
            value_class_kwargs["value"] = value
            value_class_kwargs.pop("unit")

        if self.value_class is List:
            value_class_kwargs = {"value": value}

        return self.value_class(**value_class_kwargs)

    def __eq__(self, other):
        if self.value == other.value:
            return True
        else:
            return False


class Timestamp(lineage._Value):
    """
    Timestamp

    :param value: Timestamp value
    :type value: str
    """

    def __init__(self, value, regex: str = ""):
        if "%n" in regex:
            data_type = "TIMESTAMP_NS"
        elif "%f" in regex:
            data_type = "TIMESTAMP"
        elif "%g" in regex:
            data_type = "TIMESTAMP_MS"
        elif ("%S" in regex) or ("%-S" in regex):
            data_type = "TIMESTAMP_S"
        elif ("%Z" in regex) or ("%z" in regex):
            data_type = "TIMESTAMPTZ"
        else:
            data_type = "TIMESTAMP"

        super().__init__(value=value, data_type=Datatype(data_type))


class Date(lineage._Value):

    """
    Timestamp

    :param value: Date
    :type value: str
    """

    def __init__(self, value):
        super().__init__(value=value, data_type=Datatype("DATE"))


class Integer(lineage._Value):

    """
    Integer

    :param value: Value
    :type value: int
    :param unit: Unit of value
    :type unit: units.core.Unit
    """

    def __init__(
        self,
        value: int,
        unit: Unit = Unit(),
        signed: bool = True,
        size: int = 4,
    ):
        size = int(size)

        if size not in [int(2**i) for i in range(5)]:
            raise ValueError(
                rf"size must be in: [{', '.join([str(int(2**i)) for i in range(5)])}]"
            )

        signed_str = ""

        if not signed:
            signed_str += "U"

        if size == 1:
            data_type = signed_str + "TINYINT"
        elif size == 2:
            data_type = signed_str + "SMALLINT"
        elif size == 4:
            data_type = signed_str + "INTEGER"
        elif size == 8:
            data_type = signed_str + "BIGINT"
        elif size == 16:
            data_type = signed_str + "HUGEINT"
        else:
            raise ValueError(rf"Invalid size parameter: {size}")

        super().__init__(value=value, data_type=Datatype(data_type))


class FloatingPoint(lineage._Value):

    """
    Single precision floating point

    :param value: Floating point value
    :type value: float
    :param unit: Unit of value
    :type unit: units.core.Unit
    """

    def __init__(
        self,
        value,
        precision: int = 4,
        unit: Unit = Unit(),
    ):
        if precision not in [4, 8]:
            raise ValueError(
                rf"Precision {precision} not supported: {precision} !E [4, 8]"
            )

        data_type = Datatype(rf"FLOAT{precision}")

        super().__init__(value=value, data_type=data_type, unit=unit)


class FixedPoint(lineage._Value):

    """
    Decimal precision floating point

    :param value: Floating point value
    :type value: float
    :param width: Total number of digits
    :type width: int
    :param scale: Number of digits after decimal point
    :type scale: int
    :param unit: Unit of value
    :type unit: Unit

    **Important Note: width + scale <= 32**
    """

    def __init__(
        self,
        value,
        width: int,
        scale: int,
        unit: Unit = Unit(),
    ):
        width = int(width)
        scale = int(scale)

        if scale > width:
            raise ValueError(
                rf"Invalid scale and width values - scale:{scale} !< width:{width}"
            )

        if width > 38:
            raise ValueError(rf"Max width of 38 exceeded - width:{width}")

        super().__init__(
            value=value,
            data_type=Datatype(rf"DECIMAL({width}, {scale})"),
            unit=unit,
        )


class Varchar(lineage._Value):
    def __init__(self, value, n: int = None):
        data_type = "VARCHAR"

        if n:
            data_type += rf"({str(n)})"

        super().__init__(value=value, data_type=Datatype(data_type))


class Subquery(lineage._Value):
    def __init__(self, value: lineage._Table):
        super().__init__(
            value=value,
            data_type=value.columns.list_columns()[0].data_type,
            unit=value.columns.list_columns()[0].unit,
        )

        self.name = rf"SUBQUERY - {id(self)}"


class List(lineage._Value):
    def __init__(self, value: typing.List[typing.Any]):
        if len(list(set([v.data_type.name for v in value]))) > 1:
            raise ValueError("Mixed data_types provided in values")

        super().__init__(
            value=value,
            data_type=Datatype(rf"{value[0].data_type.name}[]"),
        )

        self.name = rf"LIST - {id(self)}"


class Struct(lineage._Value):
    def __init__(self, source_dict: typing.Dict[str, typing.Any]):
        super().__init__(
            value=source_dict,
            data_type=Datatype(
                rf"STRUCT({', '.join([key + ' ' + source_dict[key].data_type.name.upper() for key in source_dict.keys()])})"
            ),
        )

        self.name = rf"STRUCT - {id(self)}"


class Interval(lineage._Value):

    """
    Interval

    Date/time interval

    :param value: Value of interval
    :type value: int
    :param unit: Unit of interval
    :type unit: units.core.Unit
    """

    def __init__(self, value: int, unit: Unit):
        super().__init__(value=value, data_type=Datatype("INTERVAL"), unit=unit)

    def __eq__(self, other):
        if self.value == other.value:
            return True
        else:
            return False


class WildCard(lineage._Value):
    def __init__(self):
        super().__init__(
            value=lineage_operators.WildCard,
            data_type=Datatype("WILDCARD"),
        )

        self.name = "*"


class Tuple(lineage._Value):
    def __init__(self, values: typing.List[typing.Any]):
        super().__init__(
            value=values,
            data_type=Datatype(rf"{values[0].data_type.value}[]"),
        )

        self.name = rf"TUPLE - {id(self)}"


class Null(lineage._Value):
    def __init__(self, data_type=None):
        super().__init__(value=None, data_type=data_type)

        self.name = rf"NULL"


class GeoCoordinate(lineage._Value):
    def __init__(self, latitude, longitude):
        super().__init__(
            value=[latitude, longitude],
            data_type=Datatype("FLOAT[]"),
        )

        self.name = rf"GeoCoordinate ({latitude}, {longitude})"


class JSON(lineage._Value):
    def __init__(self, source):
        super().__init__(value=source, data_type=Datatype("JSON"))

        self.name = rf"JSON - {id(self)}"


class Boolean(lineage._Value):
    def __init__(self, value: bool):
        super().__init__(value=value, data_type=Datatype("BOOLEAN"))


class Raw(lineage._Value):
    def __init__(self, value):
        super().__init__(value=value, data_type=None)


class Array(lineage._Value):
    def __init__(self, value):
        super().__init__(value=value, data_type=None)


class BitString(lineage._Value):
    def __init__(self, value):
        super().__init__(value=value, data_type=None)


class Blob(lineage._Value):
    def __init__(self, value):
        super().__init__(value=value, data_type=None)


class Enum(lineage._Value):
    def __init__(self, value):
        super().__init__(value=value, data_type=None)


class Map(lineage._Value):
    def __init__(self, value):
        super().__init__(value=value, data_type=None)


class Time(lineage._Value):
    def __init__(self, value):
        super().__init__(value=value, data_type=None)


class Geometry(lineage._Value):
    def __init__(self, value):
        super().__init__(value=value, data_type=None)
