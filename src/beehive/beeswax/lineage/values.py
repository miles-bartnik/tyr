from ..lineage import core as lineage
from ..lineage import operators as lineage_operators
import typing
import pandas as pd
import re


class Datatype(lineage._Value):
    def __init__(self, value):
        if type(value) is not str:
            raise ValueError("Datatype value must be str")
        super().__init__(value=value, data_type="DATATYPE")


class Limit(lineage._Value):
    def __init__(self, value):
        super().__init__(value=value, data_type=Datatype("LIMIT"))


class Interval(lineage._Value):
    def __init__(self, value: int, unit=lineage.units.core.Unit):
        super().__init__(value=value, data_type=Datatype("INTERVAL"), unit=unit)


class Timestamp(lineage._Value):
    def __init__(self, value):
        super().__init__(value=value, data_type=Datatype("TIMESTAMP"))


class Integer(lineage._Value):
    def __init__(self, value):
        super().__init__(value=value, data_type=Datatype("INTEGER"))


class Double(lineage._Value):
    def __init__(self, value, unit=lineage.units.core.Unit()):
        super().__init__(value=value, data_type=Datatype("FLOAT"), unit=unit)


class Float(lineage._Value):
    def __init__(self, value, unit=lineage.units.core.Unit()):
        super().__init__(value=value, data_type=Datatype("FLOAT"), unit=unit)


class Varchar(lineage._Value):
    def __init__(self, value):
        super().__init__(value=value, data_type=Datatype("VARCHAR"))


class Subquery(lineage._Value):
    def __init__(self, value: lineage._Table):
        super().__init__(
            value=value,
            data_type=value.columns.list_all()[0].data_type,
        )


class List(lineage._Value):
    def __init__(self, values: typing.List[typing.Any]):
        if len(list(set([value.data_type.name for value in values]))) > 1:
            raise ValueError("Mixed data_types provided in values")

        super().__init__(
            value=values,
            data_type=Datatype(rf"{values[0].data_type.name}[]"),
        )


class Struct(lineage._Value):
    def __init__(self, source_dict):
        super().__init__(
            value=source_dict,
            data_type=Datatype(
                rf"STRUCT({', '.join([key + ' ' + source_dict[key].data_type.name.upper() for key in source_dict.keys()])})"
            ),
        )


class WildCard(lineage._Value):
    def __init__(self):
        super().__init__(
            value=lineage_operators.WildCard,
            data_type=Datatype("WILDCARD"),
        )


class Tuple(lineage._Value):
    def __init__(self, values: typing.List[typing.Any]):
        super().__init__(
            value=values,
            data_type=Datatype(rf"{values[0].data_type.value}[]"),
        )


class Null(lineage._Value):
    def __init__(self, data_type=None):
        super().__init__(value=None, data_type=data_type)


class GeoCoordinate(lineage._Value):
    def __init__(self, latitude, longitude):
        super().__init__(value=[latitude, longitude], data_type=Datatype("FLOAT[]"))


class JSON(lineage._Value):
    def __init__(self, source):
        super().__init__(value=source, data_type=Datatype("JSON"))


class Boolean(lineage._Value):
    def __init__(self, value: bool):
        super().__init__(value=value, data_type=Datatype("BOOLEAN"))
