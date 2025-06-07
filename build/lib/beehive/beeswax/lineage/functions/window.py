from ...lineage import core as lineage
from ...lineage import values as lineage_values
from ...lineage import expressions as lineage_expressions
from typing import List as TypingList, Any
import re
import pandas as pd


class Lag(lineage._Function):
    def __init__(
        self,
        source,
        partition_by: lineage.PartitionBy = lineage.PartitionBy(lineage.ColumnList([])),
        order_by: lineage.OrderBy = lineage.OrderBy(columns=lineage.ColumnList([])),
    ):
        self.source = source

        super().__init__(
            name="LAG",
            args=[source],
            partition_by=partition_by,
            order_by=order_by,
            var_type=source.var_type,
            data_type=source.data_type,
            unit=source.unit,
        )


class Lead(lineage._Function):
    def __init__(
        self,
        source,
        partition_by: lineage.PartitionBy = lineage.PartitionBy(lineage.ColumnList([])),
        order_by: lineage.OrderBy = lineage.OrderBy(columns=lineage.ColumnList([])),
    ):
        self.source = source

        super().__init__(
            name="LEAD",
            args=[source],
            partition_by=partition_by,
            order_by=order_by,
            var_type=source.var_type,
            data_type=source.data_type,
            unit=source.unit,
        )


class Rank(lineage._Function):
    def __init__(
        self,
        source,
        partition_by: lineage.PartitionBy = lineage.PartitionBy(lineage.ColumnList([])),
        order_by: lineage.OrderBy = lineage.OrderBy(columns=lineage.ColumnList([])),
    ):
        self.source = source

        super().__init__(
            name="RANK",
            args=[source],
            partition_by=partition_by,
            order_by=order_by,
            var_type=source.var_type,
            data_type=source.data_type,
        )


class RowNumber(lineage._Function):
    def __init__(
        self,
        partition_by: lineage.PartitionBy = lineage.PartitionBy(lineage.ColumnList([])),
        order_by: lineage.OrderBy = lineage.OrderBy(columns=lineage.ColumnList([])),
    ):
        self.source = None

        super().__init__(
            name="ROW_NUMBER",
            args=[],
            partition_by=partition_by,
            order_by=order_by,
            var_type=None,
            data_type=lineage_values.Datatype("INTEGER"),
        )
