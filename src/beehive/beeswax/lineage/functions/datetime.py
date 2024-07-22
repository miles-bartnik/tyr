from ...lineage import core as lineage
from ...lineage import values as lineage_values
from ...lineage import expressions as lineage_expressions
from typing import List as TypingList, Any
import re
import pandas as pd


class EpochMSToTimestamp(lineage._Function):
    def __init__(
        self,
        source,
    ):
        super().__init__(
            name="EPOCH_MS",
            args=[source],
            var_type=source.var_type,
            data_type=lineage_values.Datatype("TIMESTAMP"),
        )


class EpochToTimestamp(lineage._Function):
    def __init__(
        self,
        source,
    ):
        super().__init__(
            name="TO_TIMESTAMP",
            args=[source],
            var_type=source.var_type,
            data_type=lineage_values.Datatype("TIMESTAMP"),
        )


class TimestampToEpochMS(lineage._Function):
    def __init__(
        self,
        source,
    ):
        super().__init__(name="EPOCH_MS", args=[source], var_type=source.var_type)


class StringToTimestamp(lineage._Function):
    def __init__(
        self,
        source,
        timestamp_format,
    ):
        super().__init__(
            name="STRPTIME",
            args=[source, timestamp_format],
            var_type=source.var_type,
            data_type=lineage_values.Datatype("TIMESTAMP"),
        )


class TruncateTimestamp(lineage._Function):
    def __init__(
        self,
        source,
        granularity: lineage_values.Interval,
    ):
        self.source = source
        self.granularity = granularity

        super().__init__(
            args=[granularity, source],
            name="DATE_TRUNC",
            data_type=lineage_values.Datatype("TIMESTAMP"),
        )


class DatePart(lineage._Function):
    def __init__(self, source, part: lineage_values.Varchar):
        super().__init__(
            args=[part, source],
            name="DATE_PART",
            data_type=lineage_values.Datatype("INTEGER"),
        )


class DateDiff(lineage._Function):
    def __init__(self, start, end, unit):
        super().__init__(
            args=[
                lineage_values.Varchar(unit.sub_units.sub_unit_symbol.iloc[0]),
                start,
                end,
            ],
            name="DATE_DIFF",
            data_type=lineage_values.Datatype("INTERVAL"),
            unit=unit,
        )


class DateBin(lineage._Function):
    def __init__(self, source, interval, offset=None):
        super().__init__(
            args=[arg for arg in [interval, source, offset] if arg],
            name="TIME_BUCKET",
            data_type=lineage_values.Datatype("TIMESTAMP"),
        )


class DateAdd(lineage._Function):
    def __init__(self, source, interval):
        super().__init__(
            args=[source, interval],
            name="DATE_ADD",
            data_type=source.data_type,
            var_type="timestamp",
        )
