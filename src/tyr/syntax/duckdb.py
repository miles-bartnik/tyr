# SCHEMA


def core_schema(item):
    return rf"""CREATE SCHEMA IF NOT EXISTS {item.settings.name};"""


def schema_settings(item):
    base_sql = [rf"SET {key}={value}" for key, value in item.connection.items()]

    base_sql.extend(
        [rf"""INSTALL '{package}'; LOAD '{package}'""" for package in item.extensions]
    )

    base_sql = "; ".join(base_sql)

    return base_sql


# SOURCE


def core_data_frame(item):
    return item.name


def source_column(item):
    base_sql = rf"""\"{item.source_name}\""""

    return base_sql


def source_file(item):
    if item.file_regex.value.split(".")[-1] == "geojson":
        base_sql = rf""" st_read('{item.file_regex.value}')"""
    else:
        if item.delim.value in ["t"]:
            delim = rf"\{item.delim.value}"
        elif item.delim.value == "c":
            delim = rf","
        else:
            delim = item.delim.value

        if "*" in item.file_regex.value:
            base_sql = rf""" read_csv_auto('{item.file_regex.value}', delim='{delim}', header=True, union_by_name=True)"""
        else:
            base_sql = rf""" read_csv_auto('{item.file_regex.value}', delim='{delim}', header=True)"""

    return base_sql


def columns_record(item):
    return item.name


def columns_select(item):
    base_sql = rf"""{item.source.current_table.name}.{item.source.name}"""

    return base_sql


def columns_wild_card(item):
    return "*"


def columns_core(item, alias=False):
    if "lineage.columns.WildCard" in str(type(item.source)):
        return item.name

    else:
        base_sql = item.source.sql

        if alias:
            base_sql += rf" AS {item.name}"

        return base_sql


# VALUES


def values_varchar(item):
    base_sql = rf"CAST('{item.value}' AS VARCHAR)"

    return base_sql


def values_integer(item):
    base_sql = rf"CAST({item.value} AS INTEGER)"

    return base_sql


def values_float(item):
    base_sql = rf"CAST({item.value} AS FLOAT)"

    return base_sql


def values_decimal(item):
    base_sql = rf"CAST({item.value} AS {item.data_type.name})"

    return base_sql


def values_wildcard(item):
    base_sql = rf"*"

    return base_sql


def values_interval(item):
    base_sql = (
        rf"INTERVAL '{item.value}' {item.unit.sub_units.iloc[0].unit_name.upper()}"
    )

    return base_sql


def values_list(item):
    base_sql = rf"""[{', '.join([val.sql for val in item.value])}]"""

    return base_sql


def values_tuple(item):
    base_sql = rf"""({', '.join([val.sql for val in item.value])})"""

    return base_sql


def values_subquery(item):
    base_sql = rf"""({item.value.sql})"""

    return base_sql


def values_null(item):
    base_sql = rf"""NULL"""

    return base_sql


def values_timestamp(item):
    base_sql = rf"""TIMESTAMP'{item.value}'"""

    return base_sql


def values_date(item):
    base_sql = rf"""DATE'{item.value}'"""

    return base_sql


def values_datatype(item):
    base_sql = rf"""{item.value}"""

    return base_sql


def values_unit(item):
    base_sql = rf"""{item.value}"""

    return base_sql


def values_boolean(item):
    base_sql = rf"{str(item.value).upper()}"

    return base_sql


# TABLES


def tables_core(item, ctes=True):
    if not item.ctes.is_empty and ctes:
        base_sql = rf"""
        WITH
        """ + ", ".join(
            [
                (
                    rf"""{table.name} AS ({tables_core(table, ctes=False)})"""
                    if "lineage.tables.Core" in str(type(table))
                    else rf"""{table.name} AS {table.sql}"""
                )
                for table in item.ctes.list_tables()
                if table not in item.source.ctes.list_tables()
            ]
        )

        base_sql += rf""" SELECT {"DISTINCT" if item.distinct else ""} {', '.join([columns_core(column, alias=True) if "lineage.columns.Core" in str(type(column)) else column.sql for column in item.columns.list_columns()])}"""
    else:
        base_sql = rf"""SELECT {"DISTINCT" if item.distinct else ""} {', '.join([columns_core(column, alias=True) if "lineage.columns.Core" in str(type(column)) else column.sql for column in item.columns.list_columns()])}"""

    if item.source:
        if any(
            [
                "lineage.core.RecordGenerator" in str(type(item.source)),
                "lineage.core.RecordList" in str(type(item.source)),
            ]
        ):
            base_sql += rf""" FROM ({item.source.sql}) {item.source.name}({', '.join([column for column in item.source.columns.list_names()])})"""
        elif "lineage.tables.Union" in str(type(item.source)):
            base_sql += rf""" FROM ({item.source.sql}) AS {item.source.name}"""
        else:
            base_sql += rf""" FROM {item.source.sql}"""

    if item.where_condition:
        base_sql += rf""" WHERE {item.where_condition.sql}"""

    if (item.group_by) and not (item.primary_key.is_empty):
        base_sql += rf""" GROUP BY {', '.join([column.source.sql if "lineage.columns.Select" in str(type(column)) else column.sql for column in item.primary_key.list_columns()])}"""

        if item.having_condition:
            base_sql += rf""" HAVING {item.having_condition.sql}"""

    return base_sql


def tables_subquery(item):
    if item.name:
        base_sql = rf"""({item.source.sql}) AS {item.name}"""
    else:
        base_sql = rf"""({item.source.sql})"""

    return base_sql


def tables_select(item):
    if item.schema:
        base_sql = rf"{item.schema.settings.name}.{item.source.name}"
    else:
        base_sql = rf"{item.source.name}"

    if (item.name != item.source.name) or (item.schema):
        base_sql += rf""" AS {item.name}"""

    return base_sql


def tables_temp(item):
    base_sql = rf"""CREATE TEMP TABLE {item.name} ({', '.join([column.name + " " + column.data_type for column in item.columns.list_columns()])}); INSERT INTO {item.name} ({item.source.sql}); SELECT * FROM {item.name}"""

    return base_sql


def tables_from_records(item):
    base_sql = rf"""SELECT * FROM ({item.source.sql})"""

    return base_sql


# CORE
def core_case_when(item):
    base_sql = rf"""CASE WHEN {" WHEN ".join([item.conditions[i].sql + " THEN " + item.values[i].sql for i in range(len(item.conditions))])}"""

    if item.else_value:
        base_sql += rf""" ELSE {item.else_value.sql}"""

    base_sql += rf""" END"""

    return base_sql


def core_operator(item):
    base_sql = item.name

    return base_sql


def core_order_by(item):
    base_sql = rf"""ORDER BY {', '.join([item.columns.list_columns()[i].sql + " " + item.how[i].sql for i in range(len(item.columns.list_columns()))])}"""

    return base_sql


def core_expression(item):
    base_sql = rf"""{item.left.sql} {item.operator.sql} {item.right.sql}"""

    return base_sql


def core_insert(item):
    base_sql = rf"""INSERT INTO {item.target}"""

    return base_sql


def core_condition(item):
    base_sql = item.checks[0].sql

    if len(item.checks) > 1:
        base_sql += " " + " ".join(
            [
                rf"""{item.link_operators[i].sql} {item.checks[i + 1].sql}"""
                for i in range(len(item.link_operators))
            ]
        )

    return base_sql


# JOINS


def joins_join(item):
    base_sql = rf"""{item.join_expression.sql} ON {item.condition.sql}"""

    return base_sql


def joins_compound_join(item):
    base_sql = rf"""{item.joins[0].sql}""" + " ".join(
        [
            " "
            + join.join_expression.operator.sql
            + " "
            + join.join_expression.right.sql
            + " ON "
            + join.condition.sql
            for join in item.joins[1:]
        ]
    )

    return base_sql


def values_struct(item):
    base_sql = rf"""{{{', '.join(["'" + list(item.value.keys())[i] +"':" + item.value[list(item.value.keys())[i]].sql for i in range(len(list(item.value.keys())))])}}}"""

    return base_sql


# FUNCTIONS


def core_function(item):
    base_sql = rf"""{item.name}({"DISTINCT " if item.distinct else ""}{', '.join([arg.sql for arg in item.args])})"""

    if not item.partition_by.is_empty:
        base_sql += rf""" OVER (PARTITION BY {', '.join([partition.sql for partition in item.partition_by.list_columns()])}"""

        if not item.order_by.columns.is_empty:
            base_sql += rf" {item.order_by.sql}"

        base_sql += rf""")"""

    elif not item.order_by.columns.is_empty:
        base_sql += rf""" OVER ({item.order_by.sql})"""

    return base_sql


def functions_row_number(item):
    base_sql = rf"""{item.name}({', '.join([arg.sql for arg in item.args])})"""

    if not item.partition_by.is_empty:
        base_sql += rf""" OVER (PARTITION BY {', '.join([partition.sql for partition in item.partition_by.list_columns()])}"""

        if not item.order_by.columns.is_empty:
            base_sql += rf" {item.order_by.sql}"

        base_sql += rf""")"""

    else:
        base_sql += rf""" OVER()"""

    return base_sql


def functions_to_interval(item):
    base_sql = rf"{item.args[0].sql}*{item.name} '1' {item.args[1].sub_units.iloc[0].unit_name.upper()}"

    return base_sql


def functions_list_extract(item):
    groups = []

    if any(
        [
            value not in str(type(item.args[1]))
            for value in [
                "lineage.values.List",
                "lineage.values.GeoCoordinate",
                "lineage.values.Tuple",
            ]
        ]
    ) and item.args[1].data_type.sql in ["INTEGER[]", "INT[]"]:
        values = [value.value for value in item.args[1].value]

        for i in range(len(values)):
            if i == 0:
                groups.append([values[i]])

            elif values[i] - values[i - 1] == 1:
                groups[-1].append(values[i])

            else:
                groups.append([values[i]])

        base_sql = rf"{'+'.join([item.args[0].sql + '[' + str(min(x)) + ':' + str(max(x)) + ']' if min(x) != max(x) else item.args[0].sql + '[' + str(max(x)) + ']' for x in groups])}"

        return base_sql

    else:
        raise ValueError


def functions_json_extract(item):
    return rf"{item.args[0].sql}->{item.args[1].sql}"


def functions_source_wild_to_staging(item):
    return rf"""
    "{item.args[1].column_name}"
    """


def core_unit(item):
    return item.sub_units.iloc[0].unit_name.upper()


def core_record(item):
    return (
        rf"""("""
        + ", ".join([item.values[i].sql for i in range(len(item.values))])
        + rf""")"""
    )


def core_record_list(item):
    return rf"""SELECT * FROM VALUES {', '.join([record.sql for record in item.records])} {item.name}({', '.join([column for column in item.columns.list_names()])})"""


def core_record_generator(item):
    base_sql = "SELECT * FROM VALUES "

    for record in item.generator:
        base_sql += record.sql + ", "

    return (
        base_sql.rstrip(", ")
        + rf" {item.name}({', '.join([column for column in item.columns.list_names()])})"
    )


# UNIONS


def unions_union(item):
    column_sql = rf"""SELECT {"DISTINCT" if item.distinct else ""} {', '.join([column.sql for column in item.columns.list_columns()])}"""

    if not item.ctes.is_empty:
        base_sql = rf"""
        WITH
        """ + ", ".join(
            [
                (
                    rf"""{table.name} AS ({table.sql})"""
                    if "lineage.table.Core" not in str(type(table))
                    else rf"""{table.name} AS {table.sql}"""
                )
                for table in item.ctes.list_tables()
                if table not in item.source.ctes.list_tables()
            ]
        )

        base_sql += rf""" {column_sql}"""
    else:
        base_sql = rf"""{column_sql}"""

    base_sql += rf""" FROM {' UNION BY NAME '.join(['(' + table.sql + ')' if not any(["lineage.tables.Select" in str(type(table)), "lineage.tables.Subquery" in str(type(table))]) else "SELECT  *  FROM " + table.sql if "lineage.tables.Select" in str(type(table)) else "(" + table.source.sql + ")" if "lineage.tables.Subquery" in str(type(table)) else table.sql for table in item.source.list_columns()])}"""

    if item.where_condition:
        base_sql += rf""" WHERE {item.where_condition.sql}"""

    if (item.group_by) and not (item.primary_key.is_empty):
        base_sql += rf""" GROUP BY {', '.join([column.source.sql if "lineage.columns.Core" not in str(type(column)) else column.sql for column in item.primary_key.list_columns()])}"""

        if item.having_condition:
            base_sql += rf""" HAVING {item.having_condition.sql}"""

    return base_sql


# TRANSFORMATIONS


def transformations_limit(item):
    base_sql = rf"{item.source.sql} LIMIT {item.args[0].sql}"

    if item.args[1]:
        base_sql += rf" OFFSET {item.args[1].sql}"

    return base_sql


def core_transformation(item):
    base_sql = rf"""{item.name}({item.source.file_regex.sql}"""

    if item.args.keys():
        base_sql += rf""", {','.join([key + "=" + item.args[key].sql for key in item.args.keys()])}"""

    base_sql += ")"

    return base_sql


def unions_union_column(item):
    return rf"""{item.args[0].name}"""


# DATAFRAMES


def dataframes_data_frame(item):
    return rf"""{item.name}"""


def dataframes_data_frame_column(item):
    return rf"""{item.name}"""


def dataframes_lambda_output(item):
    return rf"""{item.name}"""
