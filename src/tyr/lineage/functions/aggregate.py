from ...lineage import core as lineage
from ...lineage import values as lineage_values


class Average(lineage._Function):
    def __init__(self, source, macro_group: str = ""):
        super().__init__(
            name="AVG",
            args=[source],
            var_type=source.var_type,
            data_type=lineage_values.Datatype("FLOAT"),
            unit=source.unit,
            macro_group=macro_group,
        )


class Minimum(lineage._Function):
    def __init__(self, source, macro_group: str = ""):
        super().__init__(
            name="MIN",
            args=[source],
            data_type=source.data_type,
            var_type=source.var_type,
            unit=source.unit,
            macro_group=macro_group,
        )


class First(lineage._Function):
    def __init__(
        self,
        source,
        partition_by: lineage.PartitionBy = lineage.PartitionBy(lineage.ColumnList([])),
        order_by: lineage.OrderBy = lineage.OrderBy(columns=lineage.ColumnList([])),
        macro_group: str = "",
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
        )


class Last(lineage._Function):
    def __init__(
        self,
        source,
        partition_by: lineage.PartitionBy = lineage.PartitionBy(lineage.ColumnList([])),
        order_by: lineage.OrderBy = lineage.OrderBy(columns=lineage.ColumnList([])),
        macro_group: str = "",
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
        )


class Maximum(lineage._Function):
    def __init__(
        self,
        source,
        partition_by: lineage.PartitionBy = lineage.PartitionBy(lineage.ColumnList([])),
        order_by: lineage.OrderBy = lineage.OrderBy(columns=lineage.ColumnList([])),
        macro_group: str = "",
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
        )


class Array(lineage._Function):
    def __init__(self, source, macro_group: str = ""):
        super().__init__(
            name="ARRAY_AGG",
            args=[source],
            data_type=lineage_values.Datatype(rf"{source.data_type.value}[]"),
            var_type="array",
            unit=source.unit,
            macro_group=macro_group,
        )


class StandardDeviation(lineage._Function):
    def __init__(self, source, macro_group: str = ""):
        self.var_type = source.var_type
        self.data_type = lineage_values.Datatype("FLOAT")
        super().__init__(
            name="STDDEV", args=[source], unit=source.unit, macro_group=macro_group
        )


class PopulationStandardDeviation(lineage._Function):
    def __init__(self, source, macro_group: str = ""):
        self.var_type = source.var_type
        self.data_type = lineage_values.Datatype("FLOAT")
        super().__init__(
            name="STDDEV_POP", args=[source], unit=source.unit, macro_group=macro_group
        )


class SampleStandardDeviation(lineage._Function):
    def __init__(self, source, macro_group: str = ""):
        self.var_type = source.var_type
        self.data_type = lineage_values.Datatype("FLOAT")
        super().__init__(
            name="STDDEV_SAMP", args=[source], unit=source.unit, macro_group=macro_group
        )


class PopulationVariance(lineage._Function):
    def __init__(self, source, macro_group: str = ""):
        self.var_type = source.var_type
        self.data_type = lineage_values.Datatype("FLOAT")
        super().__init__(
            name="VAR_POP", args=[source], unit=source.unit, macro_group=macro_group
        )


class SampleVariance(lineage._Function):
    def __init__(self, source, macro_group: str = ""):
        super().__init__(
            name="VAR_SAMP",
            args=[source],
            var_type=source.var_type,
            data_type=lineage_values.Datatype("FLOAT"),
            unit=source.unit,
            macro_group=macro_group,
        )


class Sum(lineage._Function):
    def __init__(self, source, macro_group: str = ""):
        super().__init__(
            name="SUM",
            args=[source],
            var_type=source.var_type,
            data_type=lineage_values.Datatype("FLOAT"),
            unit=source.unit,
            macro_group=macro_group,
        )


class Count(lineage._Function):
    def __init__(
        self,
        source,
        distinct=False,
        partition_by: lineage.PartitionBy = lineage.PartitionBy(lineage.ColumnList([])),
        order_by: lineage.OrderBy = lineage.OrderBy(columns=lineage.ColumnList([])),
        macro_group: str = "",
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
        )


class Correlation(lineage._Function):
    def __init__(self, y, x, macro_group: str = ""):
        super().__init__(
            name="CORR",
            args=[y, x],
            var_type=y.var_type,
            data_type=lineage_values.Datatype("FLOAT"),
            macro_group=macro_group,
        )


class PopulationCovariance(lineage._Function):
    def __init__(self, y, x, macro_group: str = ""):
        super().__init__(
            name="COVAR_POP",
            args=[y, x],
            var_type=y.var_type,
            data_type=lineage_values.Datatype("FLOAT"),
            macro_group=macro_group,
        )


class SampleCovariance(lineage._Function):
    def __init__(self, y, x, macro_group: str = ""):
        super().__init__(
            name="COVAR_SAMP",
            args=[y, x],
            var_type=y.var_type,
            data_type=lineage_values.Datatype("FLOAT"),
            macro_group=macro_group,
        )


class AbsoluteMedian(lineage._Function):
    def __init__(self, source, macro_group: str = ""):
        super().__init__(
            name="MAD",
            args=[source],
            var_type=source.var_type,
            data_type=lineage_values.Datatype("FLOAT"),
            unit=source.unit,
            macro_group=macro_group,
        )


class Median(lineage._Function):
    def __init__(self, source, macro_group: str = ""):
        super().__init__(
            name="MEDIAN",
            args=[source],
            var_type=source.var_type,
            data_type=lineage_values.Datatype("FLOAT"),
            unit=source.unit,
            macro_group=macro_group,
        )
