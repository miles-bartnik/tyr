from ....lineage import core as lineage
from ....lineage import columns as lineage_columns
from ....lineage import expressions as lineage_expressions
from ....lineage import functions as lineage_functions
from ....lineage import values as lineage_values
from ....lineage import tables as lineage_tables
from ....lineage import joins as lineage_combinations
from ....lineage import operators as lineage_operators

import pandas as pd


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
            left=lineage_functions.data_type.Cast(
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
