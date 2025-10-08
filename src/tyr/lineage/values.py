from ..lineage import core as lineage
from ..lineage import operators as lineage_operators
import typing


class Datatype(lineage._Value):
    def __init__(self, value, macro_group: str = ""):
        if type(value) is not str:
            print(value)
            raise ValueError("Datatype value must be str")
        super().__init__(value=value, data_type="DATATYPE", macro_group=macro_group)


class Interval(lineage._Value):
    def __init__(self, value: int, unit=lineage.units.core.Unit, macro_group: str = ""):
        super().__init__(
            value=value,
            data_type=Datatype("INTERVAL"),
            unit=unit,
            macro_group=macro_group,
        )


class Timestamp(lineage._Value):
    def __init__(self, value, macro_group: str = ""):
        super().__init__(
            value=value, data_type=Datatype("TIMESTAMP"), macro_group=macro_group
        )


class Date(lineage._Value):
    def __init__(self, value, macro_group: str = ""):
        super().__init__(
            value=value, data_type=Datatype("DATE"), macro_group=macro_group
        )


class Integer(lineage._Value):
    def __init__(self, value, macro_group: str = ""):
        super().__init__(
            value=value, data_type=Datatype("INTEGER"), macro_group=macro_group
        )


class Double(lineage._Value):
    def __init__(self, value, unit=lineage.units.core.Unit(), macro_group: str = ""):
        super().__init__(
            value=value,
            data_type=Datatype("DOUBLE"),
            unit=unit,
            macro_group=macro_group,
        )


class Float(lineage._Value):
    def __init__(self, value, unit=lineage.units.core.Unit(), macro_group: str = ""):
        super().__init__(
            value=value, data_type=Datatype("FLOAT"), unit=unit, macro_group=macro_group
        )


class Decimal(lineage._Value):
    def __init__(
        self,
        value,
        width: int,
        scale: int,
        unit=lineage.units.core.Unit(),
        macro_group: str = "",
    ):
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
            macro_group=macro_group,
        )


class Varchar(lineage._Value):
    def __init__(self, value, macro_group: str = ""):
        super().__init__(
            value=value, data_type=Datatype("VARCHAR"), macro_group=macro_group
        )


class Subquery(lineage._Value):
    def __init__(self, value: lineage._Table, macro_group: str = ""):
        super().__init__(
            value=value,
            data_type=value.columns.list_columns()[0].data_type,
            unit=value.columns.list_columns()[0].unit,
            macro_group=macro_group,
        )

        self.name = rf"SUBQUERY - {id(self)}"


class List(lineage._Value):
    def __init__(self, values: typing.List[typing.Any], macro_group: str = ""):
        if len(list(set([value.data_type.name for value in values]))) > 1:
            raise ValueError("Mixed data_types provided in values")

        super().__init__(
            value=values,
            data_type=Datatype(rf"{values[0].data_type.name}[]"),
            macro_group=macro_group,
        )

        self.name = rf"LIST - {id(self)}"


class Struct(lineage._Value):
    def __init__(self, source_dict, macro_group: str = ""):
        super().__init__(
            value=source_dict,
            data_type=Datatype(
                rf"STRUCT({', '.join([key + ' ' + source_dict[key].data_type.name.upper() for key in source_dict.keys()])})"
            ),
            macro_group=macro_group,
        )

        self.name = rf"STRUCT - {id(self)}"


class WildCard(lineage._Value):
    def __init__(self, macro_group: str = ""):
        super().__init__(
            value=lineage_operators.WildCard,
            data_type=Datatype("WILDCARD"),
            macro_group=macro_group,
        )

        self.name = "*"


class Tuple(lineage._Value):
    def __init__(self, values: typing.List[typing.Any], macro_group: str = ""):
        super().__init__(
            value=values,
            data_type=Datatype(rf"{values[0].data_type.value}[]"),
            macro_group=macro_group,
        )

        self.name = rf"TUPLE - {id(self)}"


class Null(lineage._Value):
    def __init__(self, data_type=None, macro_group: str = ""):
        super().__init__(value=None, data_type=data_type, macro_group=macro_group)

        self.name = rf"NULL"


class GeoCoordinate(lineage._Value):
    def __init__(self, latitude, longitude, macro_group: str = ""):
        super().__init__(
            value=[latitude, longitude],
            data_type=Datatype("FLOAT[]"),
            macro_group=macro_group,
        )

        self.name = rf"GeoCoordinate ({latitude}, {longitude})"


class JSON(lineage._Value):
    def __init__(self, source, macro_group: str = ""):
        super().__init__(
            value=source, data_type=Datatype("JSON"), macro_group=macro_group
        )

        self.name = rf"JSON - {id(self)}"


class Boolean(lineage._Value):
    def __init__(self, value: bool, macro_group: str = ""):
        super().__init__(
            value=value, data_type=Datatype("BOOLEAN"), macro_group=macro_group
        )
