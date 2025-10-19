from ... import values as lineage_values
from ... import functions as lineage_functions
from ... import core as lineage
from ... import expressions as lineage_expressions
from ... import operators as lineage_operators
from ... import tables as lineage_tables
from ... import columns as lineage_columns


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


def zero_pad_timestamp(source):
    return lineage.CaseWhen(
        conditions=[
            lineage.Condition(
                checks=[
                    lineage_functions.string.RegExpMatch(
                        source=lineage_functions.string.RightExtract(
                            source, lineage_values.Integer(6)
                        ),
                        regex=lineage_values.Varchar("\s\d{2}\:\d{2}"),
                    )
                ]
            ),
            lineage.Condition(
                checks=[
                    lineage_functions.string.RegExpMatch(
                        source=lineage_functions.string.RightExtract(
                            source, lineage_values.Integer(7)
                        ),
                        regex=lineage_values.Varchar("\:\d{2}\.\d{0,2}"),
                    )
                ]
            ),
            lineage.Condition(
                checks=[
                    lineage_functions.string.RegExpMatch(
                        source=lineage_functions.string.RightExtract(
                            source, lineage_values.Integer(3)
                        ),
                        regex=lineage_values.Varchar("\:\d{2}"),
                    )
                ]
            ),
        ],
        values=[
            lineage_functions.string.Concatenate(
                [source, lineage_values.Varchar(":00.000")]
            ),
            lineage_functions.string.RegExpReplace(
                source=source,
                regex=lineage_values.Varchar("\:\d{2}\.\d{0,2}"),
                value=lineage_functions.string.RightPad(
                    lineage_functions.string.RegExpExtract(
                        source=lineage_functions.string.RightExtract(
                            source, lineage_values.Integer(7)
                        ),
                        regex=lineage_values.Varchar("\:\d{2}\.\d{0,2}"),
                        match_number=lineage_values.Integer(1),
                    ),
                    lineage_values.Integer(6),
                    lineage_values.Varchar("0"),
                ),
            ),
            lineage_functions.string.Concatenate(
                [source, lineage_values.Varchar(".000")]
            ),
        ],
        else_value=source,
    )
