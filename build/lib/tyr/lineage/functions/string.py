from ...lineage import core as lineage
from ...lineage import values as lineage_values
from typing import List as TypingList, Any


class Upper(lineage._Function):
    def __init__(self, source, macro_group: str = ""):
        super().__init__(
            name="UPPER",
            args=[source],
            var_type=source.var_type,
            data_type=source.data_type,
            macro_group=macro_group,
        )


class RegExpExtract(lineage._Function):
    def __init__(self, source, regex, match_number=None, macro_group: str = ""):
        if match_number:
            self.match_number = match_number
        else:
            match_number = lineage_values.Integer(1)

        super().__init__(
            name="REGEXP_EXTRACT",
            args=[source, regex, match_number],
            var_type=source.var_type,
            data_type=source.data_type,
            macro_group=macro_group,
        )


class RegExpExtractAll(lineage._Function):
    def __init__(self, source, regex, macro_group: str = ""):
        super().__init__(
            name="REGEXP_EXTRACT_ALL",
            args=[source, regex],
            var_type=source.var_type,
            data_type=lineage_values.Datatype(source.data_type.value + "[]"),
            macro_group=macro_group,
        )


class RegExpContains(lineage._Function):
    def __init__(self, source, regex, macro_group: str = ""):
        super().__init__(
            name="REGEXP_MATCHES",
            args=[source, regex],
            var_type="categorical",
            data_type=lineage_values.Datatype("BOOLEAN"),
            macro_group=macro_group,
        )


class RegExpMatch(lineage._Function):
    def __init__(self, source, regex, macro_group: str = ""):
        super().__init__(
            name="REGEXP_FULL_MATCH",
            args=[source, regex],
            var_type="categorical",
            data_type=lineage_values.Datatype("BOOLEAN"),
            macro_group=macro_group,
        )


class RegExpReplace(lineage._Function):
    def __init__(self, source, regex, value, macro_group: str = ""):
        super().__init__(
            name="REGEXP_REPLACE",
            args=[source, regex, value],
            data_type=lineage_values.Datatype("VARCHAR"),
            var_type=source.var_type,
            macro_group=macro_group,
        )


class Concatenate(lineage._Function):
    def __init__(
        self, strings: TypingList[Any], join_string=None, macro_group: str = ""
    ):
        if join_string:
            args = [
                x for y in [[string, join_string] for string in strings[:-1]] for x in y
            ] + [strings[:-1]]
        else:
            args = strings

        super().__init__(args=args, name="CONCAT", macro_group=macro_group)


class StringExtract(lineage._Function):
    def __init__(self, source, element: lineage_values.Integer, macro_group: str = ""):
        super().__init__(
            args=[source, element],
            name="ARRAY_EXTRACT",
            data_type=lineage_values.Datatype("VARCHAR"),
            var_type=source.var_type,
            macro_group=macro_group,
        )


class Length(lineage._Function):
    def __init__(self, source, macro_group: str = ""):
        super().__init__(
            args=[source],
            name="LENGTH",
            data_type=lineage_values.Datatype("INTEGER"),
            var_type="numeric",
            macro_group=macro_group,
        )


class Character(lineage._Function):
    def __init__(self, value: lineage_values.Integer, macro_group: str = ""):
        super().__init__(
            args=[value],
            name="CHR",
            data_type=lineage_values.Datatype("VARCHAR"),
            var_type="string",
            macro_group=macro_group,
        )


class LeftPad(lineage._Function):
    def __init__(
        self,
        source,
        length: lineage_values.Integer,
        character: lineage_values.Varchar,
        macro_group: str = "",
    ):
        super().__init__(
            args=[source, length, character],
            name="LPAD",
            data_type=lineage_values.Datatype("VARCHAR"),
            var_type="string",
            macro_group=macro_group,
        )


class RightPad(lineage._Function):
    def __init__(
        self,
        source,
        length: lineage_values.Integer,
        character: lineage_values.Varchar,
        macro_group: str = "",
    ):
        super().__init__(
            args=[source, length, character],
            name="RPAD",
            data_type=lineage_values.Datatype("VARCHAR"),
            var_type="string",
            macro_group=macro_group,
        )


class LeftExtract(lineage._Function):
    def __init__(self, source, index: lineage_values.Integer, macro_group: str = ""):
        super().__init__(
            args=[source, index],
            name="LEFT",
            data_type=lineage_values.Datatype("VARCHAR"),
            var_type="string",
            macro_group=macro_group,
        )


class RightExtract(lineage._Function):
    def __init__(self, source, index: lineage_values.Integer, macro_group: str = ""):
        super().__init__(
            args=[source, index],
            name="RIGHT",
            data_type=lineage_values.Datatype("VARCHAR"),
            var_type="string",
            macro_group=macro_group,
        )
