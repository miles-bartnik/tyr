import typing

from ...lineage import core as lineage
from ...lineage import values as lineage_values
from typing import List as TypingList, Any


class Upper(lineage._Function):
    """
    Convert a string to upper case (SQL upper).

    DuckDB: https://duckdb.org/docs/stable/sql/functions/text#upperstring
    """
    def __init__(self, source):
        super().__init__(
            name="UPPER",
            args=[source],
            var_type=source.var_type,
            data_type=source.data_type,
        )


class Lower(lineage._Function):
    """
    Convert a string to lower case (SQL lower).

    DuckDB: https://duckdb.org/docs/stable/sql/functions/text#lowerstring
    """
    def __init__(self, source):
        super().__init__(
            name="LOWER",
            args=[source],
            var_type=source.var_type,
            data_type=source.data_type,
        )


class RegExpExtract(lineage._Function):
    """
    Extract the first regex match, or a capture group (SQL regexp_extract).

    DuckDB: https://duckdb.org/docs/stable/sql/functions/regular_expressions#regexp_extractstring-pattern-group--0-options
    """
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
    """
    Extract all regex matches as a list (SQL regexp_extract_all).

    DuckDB: https://duckdb.org/docs/stable/sql/functions/regular_expressions#regexp_extract_allstring-regex-group--0-options
    """
    def __init__(self, source, regex):
        super().__init__(
            name="REGEXP_EXTRACT_ALL",
            args=[source, regex],
            var_type=source.var_type,
            data_type=lineage_values.Datatype(source.data_type.value + "[]"),
        )


class RegExpContains(lineage._Function):
    """
    Whether a regex matches anywhere in the string (SQL regexp_matches).

    DuckDB: https://duckdb.org/docs/stable/sql/functions/regular_expressions#regexp_matchesstring-pattern-options
    """
    def __init__(self, source, regex):
        super().__init__(
            name="REGEXP_MATCHES",
            args=[source, regex],
            var_type="categorical",
            data_type=lineage_values.Datatype("BOOLEAN"),
        )


class RegExpMatch(lineage._Function):
    """
    Whether a regex matches the entire string (SQL regexp_full_match).

    DuckDB: https://duckdb.org/docs/stable/sql/functions/regular_expressions#regexp_full_matchstring-regex-options
    """
    def __init__(self, source, regex):
        super().__init__(
            name="REGEXP_FULL_MATCH",
            args=[source, regex],
            var_type="categorical",
            data_type=lineage_values.Datatype("BOOLEAN"),
        )


class RegExpReplace(lineage._Function):
    """
    Replace regex matches in a string (SQL regexp_replace).

    DuckDB: https://duckdb.org/docs/stable/sql/functions/regular_expressions#regexp_replacestring-pattern-replacement-options
    """
    def __init__(self, source, regex, value):
        super().__init__(
            name="REGEXP_REPLACE",
            args=[source, regex, value],
            data_type=lineage_values.Datatype("VARCHAR"),
            var_type=source.var_type,
        )


class Concatenate(lineage._Function):
    """
    Concatenate strings together (SQL concat).

    DuckDB: https://duckdb.org/docs/stable/sql/functions/text#concatvalue-
    """
    def __init__(self, strings: TypingList[Any], join_string=None):
        if join_string:
            args = [
                x for y in [[string, join_string] for string in strings[:-1]] for x in y
            ] + [strings[:-1]]
        else:
            args = strings

        super().__init__(args=args, name="CONCAT")


class StringExtract(lineage._Function):
    """
    Extract the element at an index (SQL array_extract).

    DuckDB: https://duckdb.org/docs/stable/sql/functions/text#array_extractstring-index
    """
    def __init__(self, source, elements: lineage_values.List):
        super().__init__(
            args=[source, elements],
            name="ARRAY_EXTRACT",
            data_type=lineage_values.Datatype("VARCHAR"),
            var_type=source.var_type,
        )


class Length(lineage._Function):
    """
    Number of characters in a string (SQL length).

    DuckDB: https://duckdb.org/docs/stable/sql/functions/text#lengthstring
    """
    def __init__(self, source):
        super().__init__(
            args=[source],
            name="LENGTH",
            data_type=lineage_values.Datatype("INTEGER"),
            var_type="numeric",
        )


class Character(lineage._Function):
    """
    Character for a Unicode code point (SQL chr).

    DuckDB: https://duckdb.org/docs/stable/sql/functions/text#chrcode_point
    """
    def __init__(self, value: lineage_values.Integer):
        super().__init__(
            args=[value],
            name="CHR",
            data_type=lineage_values.Datatype("VARCHAR"),
            var_type="string",
        )


class LeftPad(lineage._Function):
    """
    Left-pad a string to a length with a fill character (SQL lpad).

    DuckDB: https://duckdb.org/docs/stable/sql/functions/text#lpadstring-count-character
    """
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
    """
    Right-pad a string to a length with a fill character (SQL rpad).

    DuckDB: https://duckdb.org/docs/stable/sql/functions/text#rpadstring-count-character
    """
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
    """
    Leftmost N characters of a string (SQL left).

    DuckDB: https://duckdb.org/docs/stable/sql/functions/text#leftstring-count
    """
    def __init__(self, source, index: lineage_values.Integer):
        super().__init__(
            args=[source, index],
            name="LEFT",
            data_type=lineage_values.Datatype("VARCHAR"),
            var_type="string",
        )


class RightExtract(lineage._Function):
    """
    Rightmost N characters of a string (SQL right).

    DuckDB: https://duckdb.org/docs/stable/sql/functions/text#rightstring-count
    """
    def __init__(self, source, index: lineage_values.Integer):
        super().__init__(
            args=[source, index],
            name="RIGHT",
            data_type=lineage_values.Datatype("VARCHAR"),
            var_type="string",
        )


class Contains(lineage._Function):
    """
    Whether a string contains a substring (SQL contains).

    DuckDB: https://duckdb.org/docs/stable/sql/functions/text#containsstring-search_string
    """
    def __init__(self, source, string: lineage_values.Varchar):
        super().__init__(
            args=[source, string],
            name="CONTAINS",
            data_type=lineage_values.Datatype("VARCHAR"),
            var_type="string",
        )
