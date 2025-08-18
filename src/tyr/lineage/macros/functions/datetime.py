from ... import values as lineage_values
from ... import functions as lineage_functions


def truncate_timestamp(source, granularity: lineage_values.Interval):
    unit_shift = {
        "milliseconds": "seconds",
        "seconds": "minutes",
        "minutes": "hours",
        "hours": "days",
        "days": "months",
        "months": "years",
        "MILLISECONDS": "seconds",
        "SECONDS": "minutes",
        "MINUTES": "hours",
        "HOURS": "days",
        "DAYS": "months",
        "MONTHS": "years",
    }

    return lineage_functions.math.Add(
        left=lineage_functions.datetime.TruncateTimestamp(
            source=source,
            granularity=lineage_values.Interval(1, unit_shift[granularity.unit.name]),
        ),
        right=lineage_functions.math.Multiply(
            left=tyr.lineage.functions.data_type.Cast(
                source=lineage_functions.math.Divide(
                    left=lineage_functions.datetime.DatePart(
                        source=source,
                        part=lineage_values.Varchar(unit_shift[granularity.unit.value]),
                    ),
                    right=lineage_values.Integer(granularity.value),
                ),
                data_type=lineage_values.Datatype("INTEGER"),
            ),
            right=granularity,
        ),
    )
