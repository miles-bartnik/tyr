from ..connections import Connection
from ...interpreter import Interpreter
import pandas as pd
from ...beeswax import lineage
from .checks import columns as column_checks
from .checks import tables as table_checks
from .checks import core as core_checks


def validate_schema(
    conn: Connection, schema: lineage.schema.Schema, interpreter: Interpreter
):
    check_list = []

    for table in schema.tables.list_all():
        check_list.extend(
            [
                table_checks.core.primary_key_completeness(
                    lineage.tables.Select(table, alias=table.name),
                    scope="general",
                )
            ]
        )

        if table.event_time:
            check_list.extend(
                [
                    table_checks.core.event_time_standard_deviation(
                        lineage.tables.Select(table, alias=table.name),
                        scope="general",
                    )
                ]
            )

        for column in table.columns.list_all():
            check_list.extend(
                [
                    column_checks.core.count(
                        lineage.columns.Select(column), scope="general"
                    ),
                    column_checks.core.count_distinct(
                        lineage.columns.Select(column), scope="general"
                    ),
                    column_checks.core.count_null(
                        lineage.columns.Select(column), scope="general"
                    ),
                    column_checks.core.proportion_null(
                        lineage.columns.Select(column), scope="general"
                    ),
                ]
            )

            if column.var_type == "numeric":
                check_list.extend(
                    [
                        column_checks.numeric.standard_deviation(
                            lineage.columns.Select(column), scope="general"
                        ),
                        column_checks.numeric.average(
                            lineage.columns.Select(column), scope="general"
                        ),
                        column_checks.numeric.maximum(
                            lineage.columns.Select(column), scope="general"
                        ),
                        column_checks.numeric.minimum(
                            lineage.columns.Select(column), scope="general"
                        ),
                    ]
                )

            elif column.var_type == "timestamp":
                check_list.extend(
                    [
                        column_checks.numeric.minimum(
                            lineage.columns.Select(column), scope="general"
                        ),
                        column_checks.numeric.maximum(
                            lineage.columns.Select(column), scope="general"
                        ),
                    ]
                )
            elif column.var_type == "array":
                check_list.extend()

    return pd.concat(
        [conn.execute(interpreter.to_query_dict(check.check)).df() for check in check_list]
    )
