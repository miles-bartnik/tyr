from ...lineage import core as lineage
from ...lineage import values as lineage_values
from ...lineage import operators


class Lag(lineage._Function):
    """
    Value from a row a given offset BEFORE the current row in the window (SQL lag).

    DuckDB: https://duckdb.org/docs/stable/sql/functions/window_functions#lagexpr-offset-default-order-by-ordering-ignore-nulls
    """
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
    """
    Value from a row a given offset AFTER the current row in the window (SQL lead).

    DuckDB: https://duckdb.org/docs/stable/sql/functions/window_functions#leadexpr-offset-default-order-by-ordering-ignore-nulls
    """
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
    """
    Rank within the window, leaving gaps after ties (SQL rank).

    DuckDB: https://duckdb.org/docs/stable/sql/functions/window_functions#rankorder-by-ordering
    """
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
    """
    Sequential row number within the window partition (SQL row_number).

    DuckDB: https://duckdb.org/docs/stable/sql/functions/window_functions#row_numberorder-by-ordering
    """
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
