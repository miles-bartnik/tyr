from ..lineage import core as lineage
from ..lineage import values as lineage_values
from typing import List, Any


class Average(lineage._Aggregate):
    def __init__(self, source):
        super().__init__(
            name="AVG",
            args=[source],
            var_type=source.var_type,
            data_type=lineage_values.Datatype("FLOAT"),
            unit=source.unit,
        )


class Minimum(lineage._Aggregate):
    def __init__(self, source):
        super().__init__(
            name="MIN",
            args=[source],
            data_type=source.data_type,
            var_type=source.var_type,
            unit=source.unit,
        )


class First(lineage._Aggregate):
    def __init__(
        self,
        source,
        partition_by: lineage.PartitionBy = lineage.PartitionBy(lineage.ColumnList([])),
        order_by: lineage.OrderBy = lineage.OrderBy(columns=lineage.ColumnList([])),
    ):
        super().__init__(
            name="FIRST",
            args=[source],
            data_type=source.data_type,
            var_type=source.var_type,
            partition_by=partition_by,
            order_by=order_by,
            unit=source.unit,
        )


class Last(lineage._Aggregate):
    def __init__(
        self,
        source,
        partition_by: lineage.PartitionBy = lineage.PartitionBy(lineage.ColumnList([])),
        order_by: lineage.OrderBy = lineage.OrderBy(columns=lineage.ColumnList([])),
    ):
        super().__init__(
            name="LAST",
            args=[source],
            data_type=source.data_type,
            var_type=source.var_type,
            unit=source.unit,
            order_by=order_by,
            partition_by=partition_by,
        )


class Maximum(lineage._Aggregate):
    def __init__(
        self,
        source,
        partition_by: lineage.PartitionBy = lineage.PartitionBy(lineage.ColumnList([])),
        order_by: lineage.OrderBy = lineage.OrderBy(columns=lineage.ColumnList([])),
    ):
        super().__init__(
            name="MAX",
            args=[source],
            data_type=source.data_type,
            var_type=source.var_type,
            unit=source.unit,
            partition_by=partition_by,
            order_by=order_by,
        )


class Array(lineage._Aggregate):
    def __init__(self, source):
        super().__init__(
            name="ARRAY_AGG",
            args=[source],
            data_type=lineage_values.Datatype(rf"{source.data_type.value}[]"),
            var_type="array",
            unit=source.unit,
        )


class StandardDeviation(lineage._Aggregate):
    def __init__(self, source):
        self.var_type = source.var_type
        self.data_type = lineage_values.Datatype("FLOAT")
        super().__init__(name="STDDEV", args=[source], unit=source.unit)


class PopulationStandardDeviation(lineage._Aggregate):
    def __init__(self, source):
        self.var_type = source.var_type
        self.data_type = lineage_values.Datatype("FLOAT")
        super().__init__(name="STDDEV_POP", args=[source], unit=source.unit)


class SampleStandardDeviation(lineage._Aggregate):
    def __init__(self, source):
        self.var_type = source.var_type
        self.data_type = lineage_values.Datatype("FLOAT")
        super().__init__(name="STDDEV_SAMP", args=[source], unit=source.unit)


class PopulationVariance(lineage._Aggregate):
    def __init__(self, source):
        self.var_type = source.var_type
        self.data_type = lineage_values.Datatype("FLOAT")
        super().__init__(name="VAR_POP", args=[source], unit=source.unit)


class SampleVariance(lineage._Aggregate):
    def __init__(self, source):
        super().__init__(
            name="VAR_SAMP",
            args=[source],
            var_type=source.var_type,
            data_type=lineage_values.Datatype("FLOAT"),
            unit=source.unit,
        )


class Sum(lineage._Aggregate):
    def __init__(self, source):
        super().__init__(
            name="SUM",
            args=[source],
            var_type=source.var_type,
            data_type=lineage_values.Datatype("FLOAT"),
            unit=source.unit,
        )


class Count(lineage._Aggregate):
    def __init__(self, source, distinct=False):
        super().__init__(
            name="COUNT",
            args=[source],
            distinct=distinct,
            var_type=source.var_type,
            data_type=lineage_values.Datatype("INTEGER"),
        )


class Correlation(lineage._Aggregate):
    def __init__(self, y, x):
        super().__init__(
            name="CORR",
            args=[y, x],
            var_type=y.var_type,
            data_type=lineage_values.Datatype("FLOAT"),
        )


class PopulationCovariance(lineage._Aggregate):
    def __init__(self, y, x):
        super().__init__(
            name="COVAR_POP",
            args=[y, x],
            var_type=y.var_type,
            data_type=lineage_values.Datatype("FLOAT"),
        )


class SampleCovariance(lineage._Aggregate):
    def __init__(self, y, x):
        super().__init__(
            name="COVAR_SAMP",
            args=[y, x],
            var_type=y.var_type,
            data_type=lineage_values.Datatype("FLOAT"),
        )


class AbsoluteMedian(lineage._Aggregate):
    def __init__(self, source):
        super().__init__(
            name="MAD",
            args=[source],
            var_type=source.var_type,
            data_type=lineage_values.Datatype("FLOAT"),
            unit=source.unit,
        )


class Median(lineage._Aggregate):
    def __init__(self, source):
        super().__init__(
            name="MEDIAN",
            args=[source],
            var_type=source.var_type,
            data_type=lineage_values.Datatype("FLOAT"),
            unit=source.unit,
        )
