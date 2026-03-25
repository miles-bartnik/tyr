import typing

from ...lineage import core as lineage
from ...lineage import values as lineage_values
from typing import List as TypingList, Any


class Upper(lineage._Function):
    def __init__(self, source):
        super().__init__(
            name="UPPER",
            args=[source],
            var_type=source.var_type,
            data_type=source.data_type,
        )


class Lower(lineage._Function):
    def __init__(self, source):
        super().__init__(
            name="LOWER",
            args=[source],
            var_type=source.var_type,
            data_type=source.data_type,
        )


class RegExpExtract(lineage._Function):
    def __init__(
        self,
        source,
        regex,
        match_number: lineage_values.Integer = None,
        name_list: typing.List[str] = None,
    ):
        if match_number:
            self.match_number = match_number
        else:
            match_number = lineage_values.Integer(0)

        super().__init__(
            name="REGEXP_EXTRACT",
            args=[source, regex, match_number],
            var_type=source.var_type,
            data_type=source.data_type,
        )


class RegExpExtractAll(lineage._Function):
    def __init__(self, source, regex):
        super().__init__(
            name="REGEXP_EXTRACT_ALL",
            args=[source, regex],
            var_type=source.var_type,
            data_type=lineage_values.Datatype(source.data_type.value + "[]"),
        )


class RegExpContains(lineage._Function):
    def __init__(self, source, regex):
        super().__init__(
            name="REGEXP_MATCHES",
            args=[source, regex],
            var_type="categorical",
            data_type=lineage_values.Datatype("BOOLEAN"),
        )


class RegExpMatch(lineage._Function):
    def __init__(self, source, regex):
        super().__init__(
            name="REGEXP_FULL_MATCH",
            args=[source, regex],
            var_type="categorical",
            data_type=lineage_values.Datatype("BOOLEAN"),
        )


class RegExpReplace(lineage._Function):
    def __init__(self, source, regex, value):
        super().__init__(
            name="REGEXP_REPLACE",
            args=[source, regex, value],
            data_type=lineage_values.Datatype("VARCHAR"),
            var_type=source.var_type,
        )


class Concatenate(lineage._Function):
    def __init__(self, strings: TypingList[Any], join_string=None):
        if join_string:
            args = [
                x for y in [[string, join_string] for string in strings[:-1]] for x in y
            ] + [strings[:-1]]
        else:
            args = strings

        super().__init__(args=args, name="CONCAT")


class StringExtract(lineage._Function):
    def __init__(self, source, elements: lineage_values.List):
        super().__init__(
            args=[source, elements],
            name="ARRAY_EXTRACT",
            data_type=lineage_values.Datatype("VARCHAR"),
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


class Character(lineage._Function):
    def __init__(self, value: lineage_values.Integer):
        super().__init__(
            args=[value],
            name="CHR",
            data_type=lineage_values.Datatype("VARCHAR"),
            var_type="string",
        )


class LeftPad(lineage._Function):
    def __init__(
        self,
        source,
        length: lineage_values.Integer,
        character: lineage_values.Varchar,
    ):
        super().__init__(
            args=[source, length, character],
            name="LPAD",
            data_type=lineage_values.Datatype("VARCHAR"),
            var_type="string",
        )


class RightPad(lineage._Function):
    def __init__(
        self,
        source,
        length: lineage_values.Integer,
        character: lineage_values.Varchar,
    ):
        super().__init__(
            args=[source, length, character],
            name="RPAD",
            data_type=lineage_values.Datatype("VARCHAR"),
            var_type="string",
        )


class LeftExtract(lineage._Function):
    def __init__(self, source, index: lineage_values.Integer):
        super().__init__(
            args=[source, index],
            name="LEFT",
            data_type=lineage_values.Datatype("VARCHAR"),
            var_type="string",
        )


class RightExtract(lineage._Function):
    def __init__(self, source, index: lineage_values.Integer):
        super().__init__(
            args=[source, index],
            name="RIGHT",
            data_type=lineage_values.Datatype("VARCHAR"),
            var_type="string",
        )


class Contains(lineage._Function):
    def __init__(self, source, string: lineage_values.Varchar):
        super().__init__(
            args=[source, string],
            name="CONTAINS",
            data_type=lineage_values.Datatype("VARCHAR"),
            var_type="string",
        )
