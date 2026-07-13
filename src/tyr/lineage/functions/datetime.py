from ...lineage import core as lineage
from ...lineage import values as lineage_values


class EpochMSToTimestamp(lineage._Function):
    """
    Convert epoch milliseconds to a TIMESTAMP (SQL epoch_ms).

    DuckDB: https://duckdb.org/docs/stable/sql/functions/timestamp#epoch_mstimestamp
    """
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
    """
    Convert epoch seconds to a TIMESTAMP WITH TIME ZONE (SQL to_timestamp).

    DuckDB: https://duckdb.org/docs/stable/sql/functions/timestamptz
    """
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
    """
    Epoch milliseconds of a TIMESTAMP (SQL epoch_ms).

    DuckDB: https://duckdb.org/docs/stable/sql/functions/timestamp#epoch_mstimestamp
    """
    def __init__(
        self,
        source,
    ):
        super().__init__(
            name="EPOCH_MS",
            args=[source],
            var_type=source.var_type,
        )


class StringToTimestamp(lineage._Function):
    """
    Parse a string into a TIMESTAMP using a strptime format (SQL strptime).

    DuckDB: https://duckdb.org/docs/stable/sql/functions/timestamp#strptimetext-format
    """
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
    """
    Truncate a timestamp to a given precision (SQL date_trunc).

    DuckDB: https://duckdb.org/docs/stable/sql/functions/timestamp#date_truncpart-timestamp
    """
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
    """
    Extract a sub-field (year, month, ...) from a timestamp (SQL date_part).

    DuckDB: https://duckdb.org/docs/stable/sql/functions/timestamp#date_partpart-timestamp
    """
    def __init__(self, source, part: lineage_values.Varchar):
        super().__init__(
            args=[part, source],
            name="DATE_PART",
            data_type=lineage_values.Datatype("INTEGER"),
        )


class DateDiff(lineage._Function):
    """
    Number of partition boundaries crossed between two timestamps (SQL date_diff).

    DuckDB: https://duckdb.org/docs/stable/sql/functions/timestamp#date_diffpart-starttimestamp-endtimestamp
    """
    def __init__(self, start, end, unit):
        super().__init__(
            args=[
                lineage_values.Varchar(unit.sub_units["unit_name"].iloc[0].upper()),
                start,
                end,
            ],
            name="DATE_DIFF",
            data_type=lineage_values.Datatype("INTERVAL"),
            unit=unit,
        )


class DateBin(lineage._Function):
    """
    Truncate a timestamp into fixed-width buckets (SQL time_bucket).

    DuckDB: https://duckdb.org/docs/stable/sql/functions/timestamp#time_bucketbucket_width-timestamp-offset
    """
    def __init__(self, source, interval, offset=None):
        super().__init__(
            args=[arg for arg in [interval, source, offset] if arg],
            name="TIME_BUCKET",
            data_type=lineage_values.Datatype("TIMESTAMP"),
        )


class DateAdd(lineage._Function):
    """
    Add an interval to a date/timestamp (SQL date_add).

    DuckDB: https://duckdb.org/docs/stable/sql/functions/date
    """
    def __init__(self, source, interval):
        super().__init__(
            args=[source, interval],
            name="DATE_ADD",
            data_type=source.data_type,
            var_type="timestamp",
        )
