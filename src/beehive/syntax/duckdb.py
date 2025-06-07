import sqlparse
from ..beeswax import lineage
from .core import selector


# SCHEMA


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
    if item.file_regex.split(".")[-1] == "geojson":
        base_sql = rf""" st_read('{item.file_regex}')"""
    elif "*" in item.file_regex:
        base_sql = rf""" read_csv_auto('{item.file_regex}', delim='{item.delim}', header=True, union_by_name=True)"""
    else:
        base_sql = rf""" read_csv_auto('{item.file_regex}', delim='{item.delim}', header=True)"""

    return base_sql


def columns_blank(item):
    return item.name


def column_select(item):
    if item.current_table:
        base_sql = rf"""{item.current_table.name}.{item.source.name}"""
    else:
        base_sql = item.source.name

    return base_sql


def columns_core(item):
    base_sql = rf"{item_to_query(item.source)}"

    if {item_to_query(item.source)} != item.name:
        base_sql += rf" AS {item.name}"

    return base_sql


# VALUES


def values_varchar(item):
    base_sql = rf"'{item.value}'"

    return base_sql


def values_integer(item):
    base_sql = rf"{item.value}"

    return base_sql


def values_float(item):
    base_sql = rf"{item.value}"

    return base_sql


def values_wildcard(item):
    base_sql = rf"*"

    return base_sql


def values_interval(item):
    base_sql = rf"INTERVAL '{item.value}' {item_to_query(item.unit)}"

    return base_sql


def values_list(item):
    base_sql = rf"""[{', '.join([item_to_query(val) for val in item.value])}]"""

    return base_sql


def values_tuple(item):
    base_sql = rf"""({', '.join([item_to_query(val) for val in item.value])})"""

    return base_sql


def values_subquery(item):
    base_sql = rf"""({item_to_query(item.value)})"""

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


def tables_core(item):
    if not item.ctes.is_empty:
        base_sql = rf"""
        WITH
        """ + ", ".join(
            [
                (
                    rf"""{table.name} AS ({item_to_query(table)})"""
                    if type(table) is lineage.tables.Core
                    else rf"""{table.name} AS {item_to_query(table)}"""
                )
                for table in item.ctes.list_all()
                if table not in item.source.ctes.list_all()
            ]
        )

        base_sql += rf""" SELECT {"DISTINCT" if item.distinct else ""} {', '.join([item_to_query(column) for column in item.columns.list_all()])}"""
    else:
        base_sql = rf"""SELECT {"DISTINCT" if item.distinct else ""} {', '.join([item_to_query(column) for column in item.columns.list_all()])}"""

    if item.source:
        if type(item.source) in [lineage.core.RecordGenerator, lineage.core.RecordList]:
            base_sql += rf""" FROM ({item_to_query(item.source)}) {item.source.name}({', '.join([column for column in item.source.columns.list_names()])})"""
        elif type(item.source) is lineage.tables.Union:
            base_sql += rf""" FROM ({item_to_query(item.source)}) AS {item.source.name}"""
        else:
            base_sql += rf""" FROM {item_to_query(item.source)}"""

    if item.where_condition:
        base_sql += rf""" WHERE {item_to_query(item.where_condition)}"""

    if (item.group_by) and not (item.primary_key.is_empty):
        base_sql += rf""" GROUP BY {', '.join([item_to_query(lineage.columns.Select(column.source)) if type(column) is not lineage.columns.Core else item_to_query(column.source) for column in item.primary_key.list_all()])}"""

        if item.having_condition:
            base_sql += rf""" HAVING {item_to_query(item.having_condition)}"""

    return base_sql


def tables_subquery(item):
    if item.name:
        base_sql = rf"""({item_to_query(item.source)}) AS {item.name}"""
    else:
        base_sql = rf"""({item_to_query(item.source)})"""

    return base_sql


def tables_select(item):
    if item.schema:
        base_sql = rf"{item.schema}.{item.source.name}"
    else:
        base_sql = rf"{item.source.name}"

    if (item.name != item.source.name) or (item.schema):
        base_sql += rf""" AS {item.name}"""

    return base_sql


def tables_temp(item):
    base_sql = rf"""CREATE TEMP TABLE {item.name} ({', '.join([column.name + " " + item_to_query(column.data_type) for column in item.columns.list_all()])}); INSERT INTO {item.name} ({item_to_query(item.source)}); SELECT * FROM {item.name}"""

    return base_sql


def tables_from_records(item):
    base_sql = rf"""SELECT * FROM ({item_to_query(item.source)})"""

    return base_sql


# CORE
def core_case_when(item):
    base_sql = rf"""CASE WHEN {" WHEN ".join([item_to_query(item.conditions[i]) + " THEN " + item_to_query(item.values[i]) for i in range(len(item.conditions))])}"""

    if item.else_value:
        base_sql += rf""" ELSE {item_to_query(item.else_value)}"""

    base_sql += rf""" END"""

    return base_sql


def core_operator(item):
    base_sql = item.name

    return base_sql


def core_order_by(item):
    base_sql = rf"""ORDER BY {', '.join([item_to_query(item.columns.list_all()[i]) + " " + item_to_query(item.how[i]) for i in range(len(item.columns.list_all()))])}"""

    return base_sql


def core_expression(item):
    base_sql = rf"""{item_to_query(item.left)} {item_to_query(item.operator)} {item_to_query(item.right)}"""

    return base_sql


def core_insert(item):
    base_sql = rf"""INSERT INTO {item.target}"""

    return base_sql


def core_condition(item):
    base_sql = item_to_query(item.checks[0])

    if len(item.checks) > 1:
        base_sql += " " + " ".join(
            [
                rf"""{item_to_query(item.link_operators[i])} {item_to_query(item.checks[i + 1])}"""
                for i in range(len(item.link_operators))
            ]
        )

    return base_sql


# JOINS


def joins_join(item):
    base_sql = rf"""{item_to_query(item.join_expression)} ON {item_to_query(item.condition)}"""

    return base_sql


def joins_compound_join(item):
    base_sql = rf"""{item_to_query(item.joins[0])}""" + " ".join(
        [
            " "
            + item_to_query(join.join_expression.operator)
            + " "
            + item_to_query(join.join_expression.right)
            + " ON "
            + item_to_query(join.condition)
            for join in item.joins[1:]
        ]
    )

    return base_sql


def values_struct(item):
    base_sql = rf"""{{{', '.join(["'"+ list(item.value.keys())[i] +"':" + item_to_query(item.value[list(item.value.keys())[i]]) for i in range(len(list(item.value.keys())))])}}}"""

    return base_sql


# FUNCTIONS


def core_function(item):
    base_sql = rf"""{item.name}({"DISTINCT " if item.distinct else ""}{', '.join([item_to_query(arg) if type(item) is not lineage.columns.Select else f"{item.current_table.name}.{item.name}" for arg in item.args])})"""

    if not item.partition_by.is_empty:
        base_sql += rf""" OVER (PARTITION BY {', '.join([item_to_query(partition) for partition in item.partition_by.list_all()])}"""

        if not item.order_by.columns.is_empty:
            base_sql += rf" {item_to_query(item.order_by)}"

        base_sql += rf""")"""

    elif not item.order_by.columns.is_empty:
        base_sql += rf""" OVER ({item_to_query(item.order_by)})"""

    return base_sql


def functions_row_number(item):
    base_sql = rf"""{item.name}({', '.join([item_to_query(arg) if type(item) is not lineage.columns.Select else f"{item.current_table.name}.{item.name}" for arg in item.args])})"""

    if not item.partition_by.is_empty:
        base_sql += rf""" OVER (PARTITION BY {', '.join([item_to_query(partition) for partition in item.partition_by.list_all()])}"""

        if not item.order_by.columns.is_empty:
            base_sql += rf" {item_to_query(item.order_by)}"

        base_sql += rf""")"""

    else:
        base_sql += rf""" OVER()"""

    return base_sql


def functions_to_interval(item):
    base_sql = rf"{item_to_query(item.args[0])}*{item.name} '1' {item_to_query(item.args[1])}"

    return base_sql


def functions_list_extract(item):
    groups = []

    if type(item.args[1]) is lineage.values.List and item_to_query(
        item.args[1].data_type
    ) in ["INTEGER[]", "INT[]"]:
        values = [value.value for value in item.args[1].value]

        for i in range(len(values)):
            if i == 0:
                groups.append([values[i]])

            elif values[i] - values[i - 1] == 1:
                groups[-1].append(values[i])

            else:
                groups.append([values[i]])

        base_sql = rf"{'+'.join([item_to_query(item.args[0]) + '[' + str(min(x)) + ':' + str(max(x)) + ']' if min(x) != max(x) else item_to_query(item.args[0]) + '[' + str(max(x)) + ']' for x in groups])}"

        return base_sql

    else:
        raise ValueError


def functions_json_extract(item):
    return rf"{item_to_query(item.args[0])}->{item_to_query(item.args[1])}"


def core_unit(item):
    return item.sub_units.iloc[0].base_unit_name.upper()


def core_record(item):
    return (
        rf"""("""
        + ", ".join([item_to_query(item.values[i]) for i in range(len(item.values))])
        + rf""")"""
    )


def core_record_list(item):
    return rf"""SELECT * FROM VALUES {', '.join([item_to_query(record) for record in item.records])} {item.name}({', '.join([column for column in item.columns.list_names()])})"""


def core_record_generator(item):
    base_sql = "SELECT * FROM VALUES "

    for record in item.generator:
        base_sql += item_to_query(record) + ", "

    return (
        base_sql.rstrip(", ")
        + rf" {item.name}({', '.join([column for column in item.columns.list_names()])})"
    )


# UNIONS


def unions_union(item):
    column_sql = rf"""SELECT {"DISTINCT" if item.distinct else ""} {', '.join([item_to_query(column) for column in item.columns.list_all()])}"""

    if not item.ctes.is_empty:
        base_sql = rf"""
        WITH
        """ + ", ".join(
            [
                (
                    rf"""{table.name} AS ({item_to_query(table)})"""
                    if type(table) is lineage.tables.Core
                    else rf"""{table.name} AS {item_to_query(table)}"""
                )
                for table in item.ctes.list_all()
                if table not in item.source.ctes.list_all()
            ]
        )

        base_sql += rf""" {column_sql}"""
    else:
        base_sql = rf"""{column_sql}"""

    base_sql += rf""" FROM {' UNION BY NAME '.join(['(' + item_to_query(table) + ')' if type(table) not in [lineage.tables.Select, lineage.tables.Subquery] else "SELECT  *  FROM " + item_to_query(table) if type(table) is lineage.tables.Select else "(" + item_to_query(table.source) + ")" if type(table) is lineage.tables.Subquery else item_to_query(table) for table in item.source.list_all()])}"""

    if item.where_condition:
        base_sql += rf""" WHERE {item_to_query(item.where_condition)}"""

    if (item.group_by) and not (item.primary_key.is_empty):
        base_sql += rf""" GROUP BY {', '.join([item_to_query(lineage.columns.Select(column.source)) if type(column) is not lineage.columns.Core else item_to_query(column.source) for column in item.primary_key.list_all()])}"""

        if item.having_condition:
            base_sql += rf""" HAVING {item_to_query(item.having_condition)}"""

    return base_sql


# TRANSFORMATIONS


def transformations_limit(item):
    base_sql = (
        rf"{item_to_query(item.source)} LIMIT {item_to_query(item.args[0])}"
    )

    if item.args[1]:
        base_sql += rf" OFFSET {item_to_query(item.args[1])}"

    return base_sql


def core_transformation(item):
    base_sql = rf"""{item.name}({item_to_query(item.source.file_regex)}"""

    if item.args.keys():
        base_sql += rf""", {','.join([key + "=" + item_to_query(item.args[key]) for key in item.args.keys()])}"""

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


# MACRO
def macros_macro(item):
    return item_to_query(item.macro)


def item_to_query(item):
    if not globals()[selector(item)](item):
        print(item)
        print(type(item))
        print(item.__dict__)
        raise ValueError
    base_sql = globals()[selector(item)](item)

    return base_sql.replace(";", "; ").replace('\\"', '"')
