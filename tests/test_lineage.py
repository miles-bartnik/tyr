import os
import tyr


def init_connection():
    if "conn" in globals().keys():
        conn.close()

    conn = tyr.database.connections.Connection(
        name="test", syntax="duckdb", database="test.duckdb"
    )

    conn.execute(source.settings.sql)

    conn.execute(staging.settings.sql)

    return conn


source = tyr.lineage.schema.source.Source(
    settings=tyr.lineage.schema.source.SourceSettings(
        file_metadata=tyr.lineage.schema.source.read_file_metadata(
            os.path.abspath(
                os.path.join(os.getcwd(), "configurations/file_metadata.tsv")
            )
        ),
        expected_column_metadata=tyr.lineage.schema.source.read_column_metadata(
            os.path.abspath(
                os.path.join(os.getcwd(), "configurations/column_metadata.tsv")
            )
        ),
        extensions=[{"name": "spatial", "origin": "duckdb"}],
    )
)

staging = tyr.lineage.schema.staging.Staging(
    source=source,
    settings=tyr.lineage.schema.staging.StagingSettings(
        name="staging", extensions=[{"name": "spatial", "origin": "duckdb"}]
    ),
)


def test_select():
    conn = init_connection()

    test = tyr.lineage.tables.Core(
        name="sessions",
        source=tyr.lineage.tables.Select(staging.tables.sessions),
        columns=tyr.lineage.core.ColumnList([tyr.lineage.columns.WildCard()]),
    )

    query = rf"""
    SELECT *
    FROM staging.sessions
    """

    assert conn.execute(test.sql).df().equals(conn.execute(query).df())

    conn.close()


def test_groupby():
    conn = init_connection()

    test = tyr.lineage.tables.Core(
        name="gear_avg_kmh",
        columns=tyr.lineage.core.ColumnList(
            [
                tyr.lineage.columns.Core(
                    source=tyr.lineage.columns.Select(
                        staging.tables.car_telemetry.columns.driver_number
                    ),
                    name=staging.tables.car_telemetry.columns.driver_number.name,
                ),
                tyr.lineage.columns.Core(
                    source=tyr.lineage.columns.Select(
                        staging.tables.car_telemetry.columns.n_gear
                    ),
                    name=staging.tables.car_telemetry.columns.n_gear.name,
                ),
                tyr.lineage.columns.Core(
                    source=tyr.lineage.functions.aggregate.Average(
                        tyr.lineage.columns.Select(
                            staging.tables.car_telemetry.columns.kmh
                        )
                    ),
                    name="average_kmh",
                ),
            ]
        ),
        source=tyr.lineage.tables.Select(staging.tables.car_telemetry),
        primary_key=tyr.lineage.core.ColumnList(
            [
                tyr.lineage.columns.Core(
                    source=tyr.lineage.columns.Select(
                        staging.tables.car_telemetry.columns.driver_number
                    ),
                    name=staging.tables.car_telemetry.columns.driver_number.name,
                ),
                tyr.lineage.columns.Core(
                    source=tyr.lineage.columns.Select(
                        staging.tables.car_telemetry.columns.n_gear
                    ),
                    name=staging.tables.car_telemetry.columns.n_gear.name,
                ),
            ]
        ),
        group_by=True,
        order_by=tyr.lineage.core.OrderBy(
            columns=tyr.lineage.core.ColumnList(
                [
                    tyr.lineage.columns.Select(
                        staging.tables.car_telemetry.columns.driver_number
                    ),
                    tyr.lineage.columns.Select(
                        staging.tables.car_telemetry.columns.n_gear
                    ),
                ]
            ),
            how=[tyr.lineage.operators.Ascending(), tyr.lineage.operators.Ascending()],
        ),
    )

    query = rf"""
    SELECT driver_number,
           n_gear,
           AVG(kmh) AS average_kmh
    FROM staging.car_telemetry
    GROUP BY 1,2
    ORDER BY 1, 2 ASC
    """

    assert conn.execute(test.sql).df().equals(conn.execute(query).df())

    conn.close()
