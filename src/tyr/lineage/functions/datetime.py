from ...lineage import core as lineage
from ...lineage import values as lineage_values


class EpochMSToTimestamp(lineage._Function):
    def __init__(
        self,
        source,
        macro_group: str = "",
    ):
        super().__init__(
            name="EPOCH_MS",
            args=[source],
            var_type=source.var_type,
            data_type=lineage_values.Datatype("TIMESTAMP"),
            macro_group=macro_group,
        )


class EpochToTimestamp(lineage._Function):
    def __init__(
        self,
        source,
        macro_group: str = "",
    ):
        super().__init__(
            name="TO_TIMESTAMP",
            args=[source],
            var_type=source.var_type,
            data_type=lineage_values.Datatype("TIMESTAMP"),
            macro_group=macro_group,
        )


class TimestampToEpochMS(lineage._Function):
    def __init__(
        self,
        source,
        macro_group: str = "",
    ):
        super().__init__(
            name="EPOCH_MS",
            args=[source],
            var_type=source.var_type,
            macro_group=macro_group,
        )


class StringToTimestamp(lineage._Function):
    def __init__(
        self,
        source,
        timestamp_format,
        macro_group: str = "",
    ):
        super().__init__(
            name="STRPTIME",
            args=[source, timestamp_format],
            var_type=source.var_type,
            data_type=lineage_values.Datatype("TIMESTAMP"),
            macro_group=macro_group,
        )


class TruncateTimestamp(lineage._Function):
    def __init__(
        self,
        source,
        granularity: lineage_values.Interval,
        macro_group: str = "",
    ):
        self.source = source
        self.granularity = granularity

        super().__init__(
            args=[granularity, source],
            name="DATE_TRUNC",
            data_type=lineage_values.Datatype("TIMESTAMP"),
            macro_group=macro_group,
        )


class DatePart(lineage._Function):
    def __init__(self, source, part: lineage_values.Varchar, macro_group: str = ""):
        super().__init__(
            args=[part, source],
            name="DATE_PART",
            data_type=lineage_values.Datatype("INTEGER"),
            macro_group=macro_group,
        )


class DateDiff(lineage._Function):
    def __init__(self, start, end, unit, macro_group: str = ""):
        super().__init__(
            args=[
                lineage_values.Varchar(unit.sub_units["unit_name"].iloc[0].upper()),
                start,
                end,
            ],
            name="DATE_DIFF",
            data_type=lineage_values.Datatype("INTERVAL"),
            unit=unit,
            macro_group=macro_group,
        )


class DateBin(lineage._Function):
    def __init__(self, source, interval, offset=None, macro_group: str = ""):
        super().__init__(
            args=[arg for arg in [interval, source, offset] if arg],
            name="TIME_BUCKET",
            data_type=lineage_values.Datatype("TIMESTAMP"),
            macro_group=macro_group,
        )


class DateAdd(lineage._Function):
    def __init__(self, source, interval, macro_group: str = ""):
        super().__init__(
            args=[source, interval],
            name="DATE_ADD",
            data_type=source.data_type,
            var_type="timestamp",
            macro_group=macro_group,
        )
