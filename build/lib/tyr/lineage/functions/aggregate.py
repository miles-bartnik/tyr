"""
Aggregate functions group multiple elements
of a column or table together to produce a result
"""

from ...lineage import core as lineage
from ...lineage import values as lineage_values
from ...lineage.expressions import Between


class Average(lineage._Function):

    """
    Take the average (arithmetic mean) of a set of values

    :param source: Source to take average from
    :type source: Any
    :param macro_group: Default: ``""``
    :type macro_group: str
    :param partition_by: Default: ``lineage.PartitionBy(lineage.ColumnList([]))``
    :type partition_by: lineage.PartitionBy
    :param order_by: Default: ``lineage.OrderBy(lineage.ColumnList([]))``
    :type order_by: lineage.OrderBy
    :param framing: Default: ``None``
    :type framing: lineage.expressions.Between
    """

    def __init__(
        self,
        source,
        macro_group: str = "",
        partition_by: lineage.PartitionBy = lineage.PartitionBy(lineage.ColumnList([])),
        order_by: lineage.OrderBy = lineage.OrderBy(lineage.ColumnList([])),
        framing: Between = None,
    ):
        super().__init__(
            name="AVG",
            args=[source],
            var_type=source.var_type,
            data_type=lineage_values.Datatype("FLOAT"),
            unit=source.unit,
            macro_group=macro_group,
            partition_by=partition_by,
            order_by=order_by,
            framing=framing,
        )


class Minimum(lineage._Function):

    """
    Take the minimum of a set of values

    :param source: Source to take average from
    :type source: Any
    :param macro_group: Default: ``""``
    :type macro_group: str
    :param partition_by: Default: ``lineage.PartitionBy(lineage.ColumnList([]))``
    :type partition_by: lineage.PartitionBy
    :param order_by: Default: ``lineage.OrderBy(lineage.ColumnList([]))``
    :type order_by: lineage.OrderBy
    :param framing: Default: ``None``
    :type framing: lineage.expressions.Between
    """

    def __init__(
        self,
        source,
        macro_group: str = "",
        partition_by: lineage.PartitionBy = lineage.PartitionBy(lineage.ColumnList([])),
        order_by: lineage.OrderBy = lineage.OrderBy(lineage.ColumnList([])),
        framing: Between = None,
    ):
        super().__init__(
            name="MIN",
            args=[source],
            data_type=source.data_type,
            var_type=source.var_type,
            unit=source.unit,
            macro_group=macro_group,
            partition_by=partition_by,
            order_by=order_by,
            framing=framing,
        )


class First(lineage._Function):

    """
    Take the first of a set of values

    :param source: Source to take average from
    :type source: Any
    :param macro_group: Default: ``""``
    :type macro_group: str
    :param partition_by: Default: ``lineage.PartitionBy(lineage.ColumnList([]))``
    :type partition_by: lineage.PartitionBy
    :param order_by: Default: ``lineage.OrderBy(lineage.ColumnList([]))``
    :type order_by: lineage.OrderBy
    :param framing: Default: ``None``
    :type framing: lineage.expressions.Between
    """

    def __init__(
        self,
        source,
        partition_by: lineage.PartitionBy = lineage.PartitionBy(lineage.ColumnList([])),
        order_by: lineage.OrderBy = lineage.OrderBy(columns=lineage.ColumnList([])),
        macro_group: str = "",
        framing: Between = None,
    ):
        super().__init__(
            name="FIRST",
            args=[source],
            data_type=source.data_type,
            var_type=source.var_type,
            partition_by=partition_by,
            order_by=order_by,
            unit=source.unit,
            macro_group=macro_group,
            framing=framing,
        )


class Last(lineage._Function):

    """
    Take the last of a set of values

    :param source: Source to take average from
    :type source: Any
    :param macro_group: Default: ``""``
    :type macro_group: str
    :param partition_by: Default: ``lineage.PartitionBy(lineage.ColumnList([]))``
    :type partition_by: lineage.PartitionBy
    :param order_by: Default: ``lineage.OrderBy(lineage.ColumnList([]))``
    :type order_by: lineage.OrderBy
    :param framing: Default: ``None``
    :type framing: lineage.expressions.Between
    """

    def __init__(
        self,
        source,
        partition_by: lineage.PartitionBy = lineage.PartitionBy(lineage.ColumnList([])),
        order_by: lineage.OrderBy = lineage.OrderBy(columns=lineage.ColumnList([])),
        macro_group: str = "",
        framing: Between = None,
    ):
        super().__init__(
            name="LAST",
            args=[source],
            data_type=source.data_type,
            var_type=source.var_type,
            unit=source.unit,
            macro_group=macro_group,
            order_by=order_by,
            partition_by=partition_by,
            framing=framing,
        )


class Maximum(lineage._Function):

    """
    Take the maximum of a set of values

    :param source: Source to take average from
    :type source: Any
    :param macro_group: Default: ``""``
    :type macro_group: str
    :param partition_by: Default: ``lineage.PartitionBy(lineage.ColumnList([]))``
    :type partition_by: lineage.PartitionBy
    :param order_by: Default: ``lineage.OrderBy(lineage.ColumnList([]))``
    :type order_by: lineage.OrderBy
    :param framing: Default: ``None``
    :type framing: lineage.expressions.Between
    """

    def __init__(
        self,
        source,
        partition_by: lineage.PartitionBy = lineage.PartitionBy(lineage.ColumnList([])),
        order_by: lineage.OrderBy = lineage.OrderBy(columns=lineage.ColumnList([])),
        macro_group: str = "",
        framing: Between = None,
    ):
        super().__init__(
            name="MAX",
            args=[source],
            data_type=source.data_type,
            var_type=source.var_type,
            unit=source.unit,
            macro_group=macro_group,
            partition_by=partition_by,
            order_by=order_by,
            framing=framing,
        )


class Array(lineage._Function):

    """
    Return the set of values as an array

    :param source: Source to take average from
    :type source: Any
    :param macro_group: Default: ``""``
    :type macro_group: str
    :param partition_by: Default: ``lineage.PartitionBy(lineage.ColumnList([]))``
    :type partition_by: lineage.PartitionBy
    :param order_by: Default: ``lineage.OrderBy(lineage.ColumnList([]))``
    :type order_by: lineage.OrderBy
    :param framing: Default: ``None``
    :type framing: lineage.expressions.Between
    """

    def __init__(
        self,
        source,
        macro_group: str = "",
        partition_by: lineage.PartitionBy = lineage.PartitionBy(lineage.ColumnList([])),
        order_by: lineage.OrderBy = lineage.OrderBy(lineage.ColumnList([])),
        framing: Between = None,
    ):
        super().__init__(
            name="ARRAY_AGG",
            args=[source],
            data_type=lineage_values.Datatype(rf"{source.data_type.value}[]"),
            var_type="array",
            unit=source.unit,
            macro_group=macro_group,
            partition_by=partition_by,
            order_by=order_by,
            framing=framing,
        )


class StandardDeviation(lineage._Function):

    """
    Take the standard deviation of a set of values

    :param source: Source to take average from
    :type source: Any
    :param macro_group: Default: ``""``
    :type macro_group: str
    :param partition_by: Default: ``lineage.PartitionBy(lineage.ColumnList([]))``
    :type partition_by: lineage.PartitionBy
    :param order_by: Default: ``lineage.OrderBy(lineage.ColumnList([]))``
    :type order_by: lineage.OrderBy
    :param framing: Default: ``None``
    :type framing: lineage.expressions.Between
    """

    def __init__(
        self,
        source,
        macro_group: str = "",
        partition_by: lineage.PartitionBy = lineage.PartitionBy(lineage.ColumnList([])),
        order_by: lineage.OrderBy = lineage.OrderBy(lineage.ColumnList([])),
        framing: Between = None,
    ):
        self.var_type = source.var_type
        self.data_type = lineage_values.Datatype("FLOAT")
        super().__init__(
            name="STDDEV",
            args=[source],
            unit=source.unit,
            macro_group=macro_group,
            partition_by=partition_by,
            order_by=order_by,
            framing=framing,
        )


class PopulationStandardDeviation(lineage._Function):

    """
    Take the population standard deviation of a set of values

    :param source: Source to take average from
    :type source: Any
    :param macro_group: Default: ``""``
    :type macro_group: str
    :param partition_by: Default: ``lineage.PartitionBy(lineage.ColumnList([]))``
    :type partition_by: lineage.PartitionBy
    :param order_by: Default: ``lineage.OrderBy(lineage.ColumnList([]))``
    :type order_by: lineage.OrderBy
    :param framing: Default: ``None``
    :type framing: lineage.expressions.Between
    """

    def __init__(
        self,
        source,
        macro_group: str = "",
        partition_by: lineage.PartitionBy = lineage.PartitionBy(lineage.ColumnList([])),
        order_by: lineage.OrderBy = lineage.OrderBy(lineage.ColumnList([])),
        framing: Between = None,
    ):
        self.var_type = source.var_type
        self.data_type = lineage_values.Datatype("FLOAT")
        super().__init__(
            name="STDDEV_POP",
            args=[source],
            unit=source.unit,
            macro_group=macro_group,
            partition_by=partition_by,
            order_by=order_by,
            framing=framing,
        )


class SampleStandardDeviation(lineage._Function):

    """
    Take the sample standard deviation of a set of values

    :param source: Source to take average from
    :type source: Any
    :param macro_group: Default: ``""``
    :type macro_group: str
    :param partition_by: Default: ``lineage.PartitionBy(lineage.ColumnList([]))``
    :type partition_by: lineage.PartitionBy
    :param order_by: Default: ``lineage.OrderBy(lineage.ColumnList([]))``
    :type order_by: lineage.OrderBy
    :param framing: Default: ``None``
    :type framing: lineage.expressions.Between
    """

    def __init__(
        self,
        source,
        macro_group: str = "",
        partition_by: lineage.PartitionBy = lineage.PartitionBy(lineage.ColumnList([])),
        order_by: lineage.OrderBy = lineage.OrderBy(lineage.ColumnList([])),
        framing: Between = None,
    ):
        self.var_type = source.var_type
        self.data_type = lineage_values.Datatype("FLOAT")
        super().__init__(
            name="STDDEV_SAMP",
            args=[source],
            unit=source.unit,
            macro_group=macro_group,
            partition_by=partition_by,
            order_by=order_by,
            framing=framing,
        )


class PopulationVariance(lineage._Function):

    """
    Take the population variance of a set of values

    :param source: Source to take average from
    :type source: Any
    :param macro_group: Default: ``""``
    :type macro_group: str
    :param partition_by: Default: ``lineage.PartitionBy(lineage.ColumnList([]))``
    :type partition_by: lineage.PartitionBy
    :param order_by: Default: ``lineage.OrderBy(lineage.ColumnList([]))``
    :type order_by: lineage.OrderBy
    :param framing: Default: ``None``
    :type framing: lineage.expressions.Between
    """

    def __init__(
        self,
        source,
        macro_group: str = "",
        partition_by: lineage.PartitionBy = lineage.PartitionBy(lineage.ColumnList([])),
        order_by: lineage.OrderBy = lineage.OrderBy(lineage.ColumnList([])),
        framing: Between = None,
    ):
        self.var_type = source.var_type
        self.data_type = lineage_values.Datatype("FLOAT")
        super().__init__(
            name="VAR_POP",
            args=[source],
            unit=source.unit,
            macro_group=macro_group,
            partition_by=partition_by,
            order_by=order_by,
            framing=framing,
        )


class SampleVariance(lineage._Function):

    """
    Take the sample variance of a set of values

    :param source: Source to take average from
    :type source: Any
    :param macro_group: Default: ``""``
    :type macro_group: str
    :param partition_by: Default: ``lineage.PartitionBy(lineage.ColumnList([]))``
    :type partition_by: lineage.PartitionBy
    :param order_by: Default: ``lineage.OrderBy(lineage.ColumnList([]))``
    :type order_by: lineage.OrderBy
    :param framing: Default: ``None``
    :type framing: lineage.expressions.Between
    """

    def __init__(
        self,
        source,
        macro_group: str = "",
        partition_by: lineage.PartitionBy = lineage.PartitionBy(lineage.ColumnList([])),
        order_by: lineage.OrderBy = lineage.OrderBy(lineage.ColumnList([])),
        framing: Between = None,
    ):
        super().__init__(
            name="VAR_SAMP",
            args=[source],
            var_type=source.var_type,
            data_type=lineage_values.Datatype("FLOAT"),
            unit=source.unit,
            macro_group=macro_group,
            partition_by=partition_by,
            order_by=order_by,
            framing=framing,
        )


class Sum(lineage._Function):

    """
    Take the sum of a set of values

    :param source: Source to take average from
    :type source: Any
    :param macro_group: Default: ``""``
    :type macro_group: str
    :param partition_by: Default: ``lineage.PartitionBy(lineage.ColumnList([]))``
    :type partition_by: lineage.PartitionBy
    :param order_by: Default: ``lineage.OrderBy(lineage.ColumnList([]))``
    :type order_by: lineage.OrderBy
    :param framing: Default: ``None``
    :type framing: lineage.expressions.Between
    """

    def __init__(
        self,
        source,
        macro_group: str = "",
        partition_by: lineage.PartitionBy = lineage.PartitionBy(lineage.ColumnList([])),
        order_by: lineage.OrderBy = lineage.OrderBy(lineage.ColumnList([])),
        framing: Between = None,
    ):
        super().__init__(
            name="SUM",
            args=[source],
            var_type=source.var_type,
            data_type=lineage_values.Datatype("FLOAT"),
            unit=source.unit,
            macro_group=macro_group,
            partition_by=partition_by,
            order_by=order_by,
            framing=framing,
        )


class Count(lineage._Function):

    """
    Return the number of values in the set

    :param source: Source to take average from
    :type source: Any
    :param distinct: Default: ``False``
    :type distinct: bool
    :param macro_group: Default: ``""``
    :type macro_group: str
    :param partition_by: Default: ``lineage.PartitionBy(lineage.ColumnList([]))``
    :type partition_by: lineage.PartitionBy
    :param order_by: Default: ``lineage.OrderBy(lineage.ColumnList([]))``
    :type order_by: lineage.OrderBy
    :param framing: Default: ``None``
    :type framing: lineage.expressions.Between
    """

    def __init__(
        self,
        source,
        distinct=False,
        partition_by: lineage.PartitionBy = lineage.PartitionBy(lineage.ColumnList([])),
        order_by: lineage.OrderBy = lineage.OrderBy(columns=lineage.ColumnList([])),
        macro_group: str = "",
        framing: Between = None,
    ):
        super().__init__(
            name="COUNT",
            args=[source],
            distinct=distinct,
            var_type=source.var_type,
            data_type=lineage_values.Datatype("INTEGER"),
            partition_by=partition_by,
            order_by=order_by,
            macro_group=macro_group,
            framing=framing,
        )


class Correlation(lineage._Function):

    """
    Return the correlation between two sets of values

    :param source: Source to take average from
    :type source: Any
    :param macro_group: Default: ``""``
    :type macro_group: str
    :param partition_by: Default: ``lineage.PartitionBy(lineage.ColumnList([]))``
    :type partition_by: lineage.PartitionBy
    :param order_by: Default: ``lineage.OrderBy(lineage.ColumnList([]))``
    :type order_by: lineage.OrderBy
    :param framing: Default: ``None``
    :type framing: lineage.expressions.Between
    """

    def __init__(
        self,
        y,
        x,
        macro_group: str = "",
        partition_by: lineage.PartitionBy = lineage.PartitionBy(lineage.ColumnList([])),
        order_by: lineage.OrderBy = lineage.OrderBy(lineage.ColumnList([])),
        framing: Between = None,
    ):
        super().__init__(
            name="CORR",
            args=[y, x],
            var_type=y.var_type,
            data_type=lineage_values.Datatype("FLOAT"),
            macro_group=macro_group,
            partition_by=partition_by,
            order_by=order_by,
            framing=framing,
        )


class PopulationCovariance(lineage._Function):

    """
    Return the population covariance between two sets of values

    :param source: Source to take average from
    :type source: Any
    :param macro_group: Default: ``""``
    :type macro_group: str
    :param partition_by: Default: ``lineage.PartitionBy(lineage.ColumnList([]))``
    :type partition_by: lineage.PartitionBy
    :param order_by: Default: ``lineage.OrderBy(lineage.ColumnList([]))``
    :type order_by: lineage.OrderBy
    :param framing: Default: ``None``
    :type framing: lineage.expressions.Between
    """

    def __init__(
        self,
        y,
        x,
        macro_group: str = "",
        partition_by: lineage.PartitionBy = lineage.PartitionBy(lineage.ColumnList([])),
        order_by: lineage.OrderBy = lineage.OrderBy(lineage.ColumnList([])),
        framing: Between = None,
    ):
        super().__init__(
            name="COVAR_POP",
            args=[y, x],
            var_type=y.var_type,
            data_type=lineage_values.Datatype("FLOAT"),
            macro_group=macro_group,
            partition_by=partition_by,
            order_by=order_by,
            framing=framing,
        )


class SampleCovariance(lineage._Function):

    """
    Return the sample covariance between two sets of values

    :param source: Source to take average from
    :type source: Any
    :param macro_group: Default: ``""``
    :type macro_group: str
    :param partition_by: Default: ``lineage.PartitionBy(lineage.ColumnList([]))``
    :type partition_by: lineage.PartitionBy
    :param order_by: Default: ``lineage.OrderBy(lineage.ColumnList([]))``
    :type order_by: lineage.OrderBy
    :param framing: Default: ``None``
    :type framing: lineage.expressions.Between
    """

    def __init__(
        self,
        y,
        x,
        macro_group: str = "",
        partition_by: lineage.PartitionBy = lineage.PartitionBy(lineage.ColumnList([])),
        order_by: lineage.OrderBy = lineage.OrderBy(lineage.ColumnList([])),
        framing: Between = None,
    ):
        super().__init__(
            name="COVAR_SAMP",
            args=[y, x],
            var_type=y.var_type,
            data_type=lineage_values.Datatype("FLOAT"),
            macro_group=macro_group,
            partition_by=partition_by,
            order_by=order_by,
            framing=framing,
        )


class AbsoluteMedian(lineage._Function):

    """
    Return the absolute median of values in the set

    :param source: Source to take average from
    :type source: Any
    :param macro_group: Default: ``""``
    :type macro_group: str
    :param partition_by: Default: ``lineage.PartitionBy(lineage.ColumnList([]))``
    :type partition_by: lineage.PartitionBy
    :param order_by: Default: ``lineage.OrderBy(lineage.ColumnList([]))``
    :type order_by: lineage.OrderBy
    :param framing: Default: ``None``
    :type framing: lineage.expressions.Between
    """

    def __init__(
        self,
        source,
        macro_group: str = "",
        partition_by: lineage.PartitionBy = lineage.PartitionBy(lineage.ColumnList([])),
        order_by: lineage.OrderBy = lineage.OrderBy(lineage.ColumnList([])),
        framing: Between = None,
    ):
        super().__init__(
            name="MAD",
            args=[source],
            var_type=source.var_type,
            data_type=lineage_values.Datatype("FLOAT"),
            unit=source.unit,
            macro_group=macro_group,
            partition_by=partition_by,
            order_by=order_by,
            framing=framing,
        )


class Median(lineage._Function):

    """
    Return the median of values in the set

    :param source: Source to take average from
    :type source: Any
    :param macro_group: Default: ``""``
    :type macro_group: str
    :param partition_by: Default: ``lineage.PartitionBy(lineage.ColumnList([]))``
    :type partition_by: lineage.PartitionBy
    :param order_by: Default: ``lineage.OrderBy(lineage.ColumnList([]))``
    :type order_by: lineage.OrderBy
    :param framing: Default: ``None``
    :type framing: lineage.expressions.Between
    """

    def __init__(
        self,
        source,
        macro_group: str = "",
        partition_by: lineage.PartitionBy = lineage.PartitionBy(lineage.ColumnList([])),
        order_by: lineage.OrderBy = lineage.OrderBy(lineage.ColumnList([])),
        framing: Between = None,
    ):
        super().__init__(
            name="MEDIAN",
            args=[source],
            var_type=source.var_type,
            data_type=lineage_values.Datatype("FLOAT"),
            unit=source.unit,
            macro_group=macro_group,
            partition_by=partition_by,
            order_by=order_by,
            framing=framing,
        )
