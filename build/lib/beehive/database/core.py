import numpy as np
import pandas as pd
import re
import logging
import logging.handlers
from pythonjsonlogger import jsonlogger
from .connections import Connection
from ..interpreter import Interpreter
from ..beeswax.lineage.schema import Schema
from ..beeswax import lineage

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


def get_build_order(schema: Schema, conn: Connection):
    print("Retrieving build order...")

    interpreter = Interpreter(conn)

    build_order = pd.DataFrame(
        columns=["table"] + schema.tables.list_names()
    ).set_index("table")

    for table in schema.tables.list_all():
        print(table.name)

        records = {"table": table.name}

        records.update({table: 0 for table in schema.tables.list_names()})

        records = [records]

        if (not build_order.empty) and (not pd.DataFrame.from_records(records).empty):
            build_order = pd.concat(
                [build_order, pd.DataFrame.from_records(records)], axis=0
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
                for node in list(interpreter.to_network(table).nodes(data=True))
                if node[1]["type"] in [str(lineage.tables.Select)]
            ]
        )

        try:
            for node in [
                node
                for node in list(interpreter.to_network(table).nodes(data=True))
                if node[1]["type"] in [str(lineage.tables.Select)]
            ]:
                print(node)
                build_order.loc[node[0], table.name] = 1
        except:
            for node in [
                node[1] for node in list(interpreter.to_network(table).nodes(data=True))
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


def purge_schema(schema: Schema, c: Connection):
    c.execute(rf"DROP SCHEMA IF EXISTS {schema.settings.name} CASCADE")


def create_tables(schema: Schema, c: Connection, skip_errors: bool = False):
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

    build_order = get_build_order(schema, conn=c)
    interpreter = Interpreter(connection=c)

    print(build_order)

    purge_schema(schema, c)
    c.execute(rf"CREATE SCHEMA {schema.settings.name}")
    c.execute(interpreter.to_sql(schema.settings))

    for table in build_order:
        if skip_errors:
            try:
                print(
                    rf"DROP TABLE IF EXISTS {schema.settings.name}.{table}; CREATE TABLE {schema.settings.name}.{table} AS {interpreter.to_sql(schema.tables[table])}"
                )

                c.execute(
                    rf"DROP TABLE IF EXISTS {schema.settings.name}.{table}; CREATE TABLE {schema.settings.name}.{table} AS {interpreter.to_sql(schema.tables[table])}"
                )
            except:
                print(rf"""Error encountered in creation of {table}""")
        else:
            print(
                rf"DROP TABLE IF EXISTS {schema.settings.name}.{table}; CREATE TABLE {schema.settings.name}.{table} AS {interpreter.to_sql(schema.tables[table])}"
            )

            c.execute(
                rf"DROP TABLE IF EXISTS {schema.settings.name}.{table}; CREATE TABLE {schema.settings.name}.{table} AS {interpreter.to_sql(schema.tables[table])}"
            )
    # Finish

    # logging.log(
    #     logging.INFO, {"end_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")}
    # )
    #
    # logger.removeHandler(handler)
