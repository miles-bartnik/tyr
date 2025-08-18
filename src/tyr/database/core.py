import numpy as np
import pandas as pd
import re
import logging.handlers
from pythonjsonlogger import jsonlogger
from .connections import Connection
from tyr.lineage.schema.core import _Schema
from tyr import lineage

logger = logging.getLogger()
formatter = jsonlogger.JsonFormatter()


def get_run_id(delim: str):
    run_id = f"{delim}".join(
        [
            "".join([np.base_repr(np.random.randint(0, 16), 16) for i in range(8)]),
            "".join([np.base_repr(np.random.randint(0, 16), 16) for i in range(4)]),
            "".join([np.base_repr(np.random.randint(0, 16), 16) for i in range(4)]),
            "".join([np.base_repr(np.random.randint(0, 16), 16) for i in range(4)]),
            "".join([np.base_repr(np.random.randint(0, 16), 16) for i in range(12)]),
        ]
    ).lower()

    return run_id


def normalize_column_name(column: str):
    return re.sub("\.", "_", re.sub("_+", "_", column)).lower()


def get_build_order(schema: _Schema):
    spider = lineage.core.Spider()

    print("Retrieving build order...")

    build_order = pd.DataFrame(
        columns=["table"] + schema.tables.list_names()
    ).set_index("table")

    for table in schema.tables.list_all():
        print(table.name)

        records = {"table": table.name}

        records.update({table: 0 for table in schema.tables.list_names()})

        records = [records]

        if (not build_order.empty) and (not records):
            build_order = pd.concat(
                [build_order, pd.DataFrame.from_records(records)], axis=1
            )
        elif not pd.DataFrame.from_records(records).empty:
            build_order = pd.DataFrame.from_records(records)

    build_order = build_order.set_index("table")

    print("ITERATING THROUGH SCHEMA")
    for table in schema.tables.list_all():
        print(table.name)

        print(
            [
                node
                for node in list(spider.item_to_graph(table).nodes(data=True))
                if node[1]["type"] in [str(lineage.tables.Select)]
            ]
        )

        try:
            for node in [
                node
                for node in list(spider.item_to_graph(table).nodes(data=True))
                if node[1]["type"] in [str(lineage.tables.Select)]
            ]:
                print(node)
                build_order.loc[node[0], table.name] = 1
        except:
            for node in [
                node[1] for node in list(spider.item_to_graph(table).nodes(data=True))
            ]:
                print(node)

    build_order = build_order[build_order.sum().sort_values().index.tolist()].reindex(
        build_order.sum().sort_values().index.tolist()
    )

    build_order = build_order.index.tolist()

    return build_order


def purge(c: Connection, verify: bool = True):
    if verify:
        if (
            input(
                "Are you sure you want to purge all tables in the database?: YES/[NO]"
            )
            == "YES"
        ):
            verify = True
        else:
            verify = False
    else:
        verify = True

    if verify:
        schema_names = (
            c.execute(
                "SELECT DISTINCT schema_name FROM information_schema.schemata WHERE schema_name NOT IN ('information_schema', 'main', 'pg_catalog')"
            )
            .df()
            .schema_name.unique()
        )

        for schema in schema_names:
            c.execute(rf"DROP SCHEMA IF EXISTS {schema} CASCADE")
    else:
        print("ABORTING PURGE")


def purge_schema(schema: _Schema, c: Connection):
    c.execute(rf"DROP SCHEMA IF EXISTS {schema.settings.name} CASCADE")


def create_tables(
    schema: _Schema,
    conn: Connection,
    overwrite: bool = True,
    skip_errors: bool = False,
):
    """
    Parameters:
        - config:Dict[str,Any] - Python dictionary from yaml config.
        - c:duckdb:duckdb.DuckDBPyConnection=c - DuckDB connection.
    Returns:
    Raises:
    Description:
        - Create all tables in the provided config.
    See Also:
    """

    # Output path for log file
    # component_log_path = config["settings"]["component_log_path"]

    # # Initialize logger
    # handler = logging.FileHandler(component_log_path)
    # logger.addHandler(handler)
    # logger.setLevel(logging.INFO)
    # handler.setFormatter(formatter)
    #
    # # Log config settings
    # logging.log(logging.INFO, {"settings": config["settings"]})
    #
    # # Log start time
    # logging.log(
    #     logging.INFO, {"start_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")}
    # )

    build_order = get_build_order(schema)

    print(build_order)

    if overwrite:
        purge_schema(schema, conn)
        conn.execute(rf"CREATE SCHEMA {schema.settings.name}")
        conn.execute(schema.settings.sql)

    else:
        conn.execute(schema.settings.sql)

        pass_tables = (
            conn.execute(
                rf"""
        SELECT name FROM 
        (SHOW ALL TABLES) WHERE schema = '{schema.settings.name}'
        """
            )
            .df()["name"]
            .tolist()
        )

        print(pass_tables)

        print(build_order)

        build_order = [table for table in build_order if table not in pass_tables]

    for table in build_order:
        if skip_errors:
            try:
                print(
                    rf"DROP TABLE IF EXISTS {schema.settings.name}.{table}; CREATE TABLE {schema.settings.name}.{table} AS {schema.tables[table].sql}"
                )

                conn.execute(rf"DROP TABLE IF EXISTS {schema.settings.name}.{table}")

                conn.execute(
                    rf"""
                    PRAGMA enable_profiling='json';
                    PRAGMA profiling_output='/home/miles/F1/profile_staging_car_telemetry.json';
                    PRAGMA custom_profiling_settings = '{{"CPU_TIME": "false", "EXTRA_INFO": "true", "OPERATOR_CARDINALITY": "true", "OPERATOR_TIMING": "true"}}';
                    
                    EXPLAIN (ANALYZE, format json) {schema.tables[table].sql}
                """
                )

                conn.execute(
                    rf"""
                CREATE TABLE {schema.settings.name}.{table} AS {schema.tables[table].sql}
                """
                )
            except:
                print(rf"""Error encountered in creation of {table}""")
        else:
            print(
                rf"DROP TABLE IF EXISTS {schema.settings.name}.{table}; CREATE TABLE {schema.settings.name}.{table} AS {schema.tables[table].sql}"
            )

            conn.execute(
                rf"DROP TABLE IF EXISTS {schema.settings.name}.{table}; CREATE TABLE {schema.settings.name}.{table} AS {schema.tables[table].sql}"
            )
    # Finish

    # logging.log(
    #     logging.INFO, {"end_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")}
    # )
    #
    # logger.removeHandler(handler)
