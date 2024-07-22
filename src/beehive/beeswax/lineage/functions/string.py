from ...lineage import core as lineage
from ...lineage import values as lineage_values
from ...lineage import expressions as lineage_expressions
from typing import List as TypingList, Any
import re
import pandas as pd


class Upper(lineage._Function):
    def __init__(
        self,
        source,
    ):
        super().__init__(
            name="UPPER",
            args=[source],
            var_type=source.var_type,
            data_type=source.data_type,
        )


class RegExpExtract(lineage._Function):
    def __init__(
        self,
        source,
        regex,
        match_number=None,
    ):
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
    def __init__(self, source, elements):
        if type(elements) is not lineage_values.List:
            raise ValueError("elements must be lineage_values.List")

        if any([type(value) is not lineage_values.Integer for value in elements.value]):
            raise ValueError("All elements must be lineage_values.Integer")

        super().__init__(
            args=[source, elements],
            name="STRING_EXTRACT",
            data_type=lineage_values.Datatype("VARCHAR"),
            var_type=source.var_type,
        )
