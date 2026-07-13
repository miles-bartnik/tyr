from ...lineage import core as lineage
from ...lineage import values as lineage_values


class Length(lineage._Function):

    """
        Return the length of an array

        :param source: Source to take average from
        :type source: List

    DuckDB: https://duckdb.org/docs/stable/sql/functions/list#lengthlist
    """

    def __init__(self, source):
        super().__init__(
            args=[source],
            name="LENGTH",
            data_type=lineage_values.Datatype("INTEGER"),
            var_type="numeric",
        )


class QuantileCont(lineage._Function):

    """
        Return the quantile of an array

        :param source: Source to take quantile from
        :type source: List
        :param quantile: Quantile value
        :type source: FloatingPoint

    DuckDB: https://duckdb.org/docs/stable/sql/functions/aggregates#quantile_contx-pos
    """

    def __init__(
        self,
        source,
        quantile:lineage_values.FloatingPoint,
    ):
        super().__init__(
            args=[source, quantile],
            name="QUANTILE_CONT",
            data_type=lineage_values.Datatype("INTEGER"),
            var_type="numeric",
        )


class Unnest(lineage._Function):

    """
        Unnest a List or Array into a column

        :param source: Source List or Array
        :type source: List/Array

    DuckDB: https://duckdb.org/docs/stable/sql/functions/list#unnestlist
    """

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

    """
        Return an array of values between the start and end
        values spaced by an interval

        :param start: Start value
        :type start: Integer/FloatingPoint/Datetime
        :param end: End value
        :type end: Integer/FloatingPoint/Datetime
        :param interval: Start value
        :type interval: Integer/FloatingPoint/Interval

    DuckDB: https://duckdb.org/docs/stable/sql/functions/list#rangestart-stop-step
    """

    def __init__(self, start, end, interval):
        self.start = start
        self.end = end
        self.interval = interval

        super().__init__(
            name="RANGE",
            args=[start, end, interval],
            var_type=start.var_type,
            data_type=start.data_type,
        )


class List(lineage._Function):

    """
        Send a list of values to a List variable

        :param values: Values to convert to a List
        :type values: [Value]

    DuckDB: https://duckdb.org/docs/stable/sql/functions/list
    """

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

    """
        Extract a number of elements from a List

        :param source: Source List or Array
        :type source: List/Array
        :param elements: List of integer values to extract
        :type elements: List[Integer]

    DuckDB: https://duckdb.org/docs/stable/sql/functions/list#list_extractlist-index
    """

    def __init__(self, source, elements):
        if any([type(value) is not lineage_values.Integer for value in elements.value]):
            raise ValueError("All elements must be lineage_values.Integer")

        super().__init__(
            args=[source, elements],
            name="LIST_EXTRACT",
            data_type=lineage_values.Datatype(source.data_type.value.strip("[]")),
            var_type=source.var_type,
        )


class Maximum(lineage._Function):
    """
    Return the largest value in the list (SQL list_max).

    DuckDB: https://duckdb.org/docs/stable/sql/functions/list#list_maxlist
    """
    def __init__(
        self,
        source,
    ):
        super().__init__(
            name="LIST_MAX",
            args=[source],
            data_type=lineage_values.Datatype(source.data_type.value.strip("[]")),
            var_type=source.var_type,
        )


class Minimum(lineage._Function):
    """
    Return the smallest value in the list (SQL list_min).

    DuckDB: https://duckdb.org/docs/stable/sql/functions/list#list_minlist
    """
    def __init__(
        self,
        source,
    ):
        super().__init__(
            name="LIST_MIN",
            args=[source],
            data_type=lineage_values.Datatype(source.data_type.value.strip("[]")),
            var_type=source.var_type,
        )


class Contains(lineage._Function):
    """
    Return whether the list contains the element (SQL list_contains).

    DuckDB: https://duckdb.org/docs/stable/sql/functions/list#list_containslist-element
    """
    def __init__(self, source, element):
        super().__init__(
            name="LIST_CONTAINS",
            args=[source, element],
            data_type=lineage_values.Datatype("BOOLEAN"),
            var_type=source.var_type,
        )
