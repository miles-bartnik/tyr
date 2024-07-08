import sqlparse
from ..beeswax import lineage
from .core import selector


# SCHEMA


def schema_settings(item, expand: bool = False):
    base_sql = [rf"SET {key}={value}" for key, value in item.connection.items()]

    base_sql.extend(
        [rf"""INSTALL '{package}'; LOAD '{package}'""" for package in item.extensions]
    )

    base_sql = "; ".join(base_sql)

    return base_sql


# SOURCE


def core_data_frame(item, expand: bool = False):
    return item.name


def source_column(item, expand: bool = False):
    base_sql = rf"""\"{item.source_name}\""""

    return base_sql


def source_file(item, expand: bool = False):
    if item.file_regex.split(".")[-1] == "geojson":
        base_sql = rf""" st_read('{item.file_regex}')"""
    elif "*" in item.file_regex:
        base_sql = rf""" read_csv_auto('{item.file_regex}', delim='{item.delim}', header=True, union_by_name=True)"""
    else:
        base_sql = rf""" read_csv_auto('{item.file_regex}', delim='{item.delim}', header=True)"""

    return base_sql


def columns_blank(item, expand: bool = False):
    return item.name


def column_select(item, expand: bool = False):
    if item.is_outer:
        if item.source_table:
            base_sql = (
                rf"""{item.source_table.name}.{item.source.name} AS {item.name}"""
            )
        else:
            base_sql = rf"{item.source.name} AS {item.name}"
    else:
        if item.current_table:
            base_sql = rf"""{item.current_table.name}.{item.source.name}"""
        else:
            base_sql = item.source.name

    return base_sql


def column_expand(item, expand: bool = False):
    base_sql = rf"{item_to_query(item.source, expand)}"

    if item.is_outer:
        base_sql += rf" AS {item.name}"

    return base_sql


# VALUES


def values_varchar(item, expand: bool = False):
    base_sql = rf"'{item.value}'"

    return base_sql


def values_integer(item, expand: bool = False):
    base_sql = rf"{item.value}"

    return base_sql


def values_float(item, expand: bool = False):
    base_sql = rf"{item.value}"

    return base_sql


def values_wildcard(item, expand: bool = False):
    base_sql = rf"*"

    return base_sql


def values_interval(item, expand: bool = False):
    base_sql = rf"INTERVAL '{item.value}' {item_to_query(item.unit)}"

    return base_sql


def values_list(item, expand: bool = False):
    base_sql = (
        rf"""([{', '.join([item_to_query(val, expand) for val in item.value])}])"""
    )

    return base_sql


def values_tuple(item, expand: bool = False):
    base_sql = rf"""({', '.join([item_to_query(val, expand) for val in item.value])})"""

    return base_sql


def values_subquery(item, expand: bool = False):
    base_sql = rf"""({item_to_query(item.value, expand)})"""

    return base_sql


def values_null(item, expand: bool = False):
    base_sql = rf"""NULL"""

    return base_sql


def values_limit(item, expand: bool = False):
    base_sql = rf"""LIMIT {item.value}"""

    return base_sql


def values_timestamp(item, expand: bool = False):
    base_sql = rf"""TIMESTAMP'{item.value}'"""

    return base_sql


def values_datatype(item, expand: bool = False):
    base_sql = rf"""{item.value}"""

    return base_sql


def values_unit(item, expand: bool = False):
    base_sql = rf"""{item.value}"""

    return base_sql


def values_boolean(item, expand: bool = False):
    base_sql = rf"{str(item.value).upper()}"

    return base_sql


# TABLES


def tables_core(item, expand: bool = False):
    if not item.ctes.is_empty:
        base_sql = rf"""
        WITH
        """ + ", ".join(
            [
                (
                    rf"""{table.name} AS ({item_to_query(table, expand)})"""
                    if type(table) is lineage.tables.Core
                    else rf"""{table.name} AS {item_to_query(table, expand)}"""
                )
                for table in item.ctes.list_all()
                if table not in item.source.ctes.list_all()
            ]
        )

        base_sql += rf""" SELECT {"DISTINCT" if item.distinct else ""} {', '.join([item_to_query(column, expand) for column in item.columns.list_all()])}"""
    else:
        base_sql = rf"""SELECT {"DISTINCT" if item.distinct else ""} {', '.join([item_to_query(column, expand) for column in item.columns.list_all()])}"""

    if item.source:
        if type(item.source) in [lineage.core.RecordGenerator, lineage.core.RecordList]:
            base_sql += rf""" FROM ({item_to_query(item.source, expand)}) {item.source.name}({', '.join([column for column in item.source.columns.list_names()])})"""
        elif type(item.source) in [lineage.combinations.Union]:
            base_sql += (
                rf""" FROM {item_to_query(item.source, expand)} AS {item.name}"""
            )
        else:
            base_sql += rf""" FROM {item_to_query(item.source, expand)}"""

    if item.where_condition:
        base_sql += rf""" WHERE {item_to_query(item.where_condition)}"""

    if (item.group_by) and not (item.primary_key.is_empty):
        base_sql += rf""" GROUP BY {', '.join([item_to_query(lineage.columns.Select(column.source), expand) if type(column) is not lineage.columns.Expand else item_to_query(column.source) for column in item.primary_key.list_all()])}"""

        if item.having_condition:
            base_sql += rf""" HAVING {item_to_query(item.having_condition, expand)}"""

    return base_sql


def tables_subquery(item, expand: bool = False):
    if item.name:
        base_sql = rf"""({item_to_query(item.source, expand)}) AS {item.name}"""
    else:
        base_sql = rf"""({item_to_query(item.source, expand)})"""

    return base_sql


def tables_select(item, expand: bool = False):
    if item.schema:
        base_sql = rf"{item.schema}.{item.source.name}"
    else:
        base_sql = rf"{item.source.name}"

    if (item.name != item.source.name) or (item.schema):
        base_sql += rf""" AS {item.name}"""

    return base_sql


def tables_temp(item, expand: bool = False):
    base_sql = rf"""CREATE TEMP TABLE {item.name} ({', '.join([column.name + " " + item_to_query(column.data_type) for column in item.columns.list_all()])}); INSERT INTO {item.name} ({item_to_query(item.source)}); SELECT * FROM {item.name}"""

    return base_sql


def tables_from_records(item, expand: bool = False):
    base_sql = rf"""SELECT * FROM ({item_to_query(item.source)})"""

    return base_sql


# CORE
def core_case_when(item, expand: bool = False):
    base_sql = rf"""CASE WHEN {" WHEN ".join([item_to_query(item.conditions[i], expand) + " THEN " + item_to_query(item.values[i], expand) for i in range(len(item.conditions))])}"""

    if item.else_value:
        base_sql += rf""" ELSE {item_to_query(item.else_value, expand)}"""

    base_sql += rf""" END"""

    return base_sql


def core_operator(item, expand: bool = False):
    base_sql = item.name

    return base_sql


def core_order_by(item, expand: bool = False):
    base_sql = rf"""ORDER BY {' '.join([item_to_query(item.columns.list_all()[i], expand) + " " + item_to_query(item.how[i], expand) for i in range(len(item.columns.list_all()))])}"""

    return base_sql


def core_expression(item, expand: bool = False):
    base_sql = rf"""{item_to_query(item.left, expand=expand)} {item_to_query(item.operator, expand=expand)} {item_to_query(item.right, expand=expand)}"""

    return base_sql


def core_insert(item, expand: bool = False):
    base_sql = rf"""INSERT INTO {item.target}"""

    return base_sql


def core_condition(item, expand: bool = False):
    base_sql = item_to_query(item.checks[0], expand)

    if len(item.checks) > 1:
        base_sql += " " + " ".join(
            [
                rf"""{item_to_query(item.link_operators[i], expand)} {item_to_query(item.checks[i + 1], expand)}"""
                for i in range(len(item.link_operators))
            ]
        )

    return base_sql


# COMBINATIONS


def combinations_union(item, expand: bool = False):
    base_sql = rf"""({' UNION '.join([item_to_query(table, expand) for table in item.tables.list_all()])})"""
    return base_sql


def combinations_join(item, expand: bool = False):
    base_sql = rf"""{item_to_query(item.join_expression, expand)} ON {item_to_query(item.condition, expand)}"""

    return base_sql


def combinations_join_list(item, expand: bool = False):
    base_sql = rf"""{item_to_query(item.joins[0], expand)}""" + " ".join(
        [
            " "
            + item_to_query(join.join_expression.operator, expand)
            + " "
            + item_to_query(join.join_expression.right, expand)
            + " ON "
            + item_to_query(join.condition, expand)
            for join in item.joins[1:]
        ]
    )

    return base_sql


def values_struct(item, expand: bool = False):
    base_sql = rf"""{{{', '.join(["'"+ list(item.value.keys())[i] +"':" + item_to_query(item.value[list(item.value.keys())[i]], expand) for i in range(len(list(item.value.keys())))])}}}"""

    return base_sql


# FUNCTIONS


def core_function(item, expand: bool = False):
    base_sql = rf"""{item.name}({', '.join([item_to_query(arg, expand) if type(item) is not lineage.columns.Select else f"{item.current_table.name}.{item.name}" for arg in item.args])})"""

    if not item.partition_by.is_empty:
        base_sql += rf""" OVER (PARTITION BY {', '.join([item_to_query(partition, expand) for partition in item.partition_by.list_all()])}"""

        if not item.order_by.columns.is_empty:
            base_sql += rf" {item_to_query(item.order_by, expand)}"

        base_sql += rf""")"""

    elif not item.order_by.columns.is_empty:
        base_sql += rf""" OVER ({item_to_query(item.order_by, expand)})"""

    return base_sql


def functions_row_number(item, expand: bool = False):
    base_sql = rf"""{item.name}({', '.join([item_to_query(arg, expand) if type(item) is not lineage.columns.Select else f"{item.current_table.name}.{item.name}" for arg in item.args])})"""

    if not item.partition_by.is_empty:
        base_sql += rf""" OVER (PARTITION BY {', '.join([item_to_query(partition, expand) for partition in item.partition_by.list_all()])}"""

        if not item.order_by.columns.is_empty:
            base_sql += rf" {item_to_query(item.order_by, expand)}"

        base_sql += rf""")"""

    else:
        base_sql += rf""" OVER()"""

    return base_sql


def functions_to_interval(item, expand: bool = False):
    base_sql = rf"{item_to_query(item.args[0], expand)}*{item.name} '1' {item_to_query(item.args[1], expand)}"

    return base_sql


def functions_list_extract(item, expand: bool = False):
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

        base_sql = rf"{'+'.join([item_to_query(item.args[0], expand) + '[' + str(min(x)) + ':' + str(max(x)) + ']' if min(x) != max(x) else item_to_query(item.args[0], expand) + '[' + str(max(x)) + ']' for x in groups])}"

        return base_sql


def functions_json_extract(item, expand: bool = False):
    return rf"{item_to_query(item.args[0])}->{item_to_query(item.args[1])}"


# AGGREGATES


def core_aggregate(item, expand: bool = False):
    base_sql = rf"""
    {item.name}({"DISTINCT " if item.distinct else ""}{', '.join([item_to_query(arg, expand) if type(item) is not lineage.columns.Select else f"{item.current_table.name}.{item.name}" for arg in item.args])})"""

    if not item.partition_by.is_empty:
        base_sql += rf""" OVER (PARTITION BY {', '.join([item_to_query(partition, expand) for partition in item.partition_by.list_all()])}"""

        if not item.order_by.columns.is_empty:
            base_sql += rf" {item_to_query(item.order_by, expand)}"

        base_sql += rf""")"""

    return base_sql


def core_unit(item, expand):
    return item.sub_units.iloc[0].base_unit_name.upper()


def core_record(item, expand):
    return (
        rf"""("""
        + ", ".join([item_to_query(item.values[i]) for i in range(len(item.values))])
        + rf""")"""
    )


def core_record_list(item, expand):
    return rf"""SELECT * FROM VALUES {', '.join([item_to_query(record) for record in item.records])} {item.name}({', '.join([column for column in item.columns.list_names()])})"""


def core_record_generator(item, expand):
    base_sql = "SELECT * FROM VALUES "

    for record in item.generator:
        base_sql += item_to_query(record) + ", "

    return (
        base_sql.rstrip(", ")
        + rf" {item.name}({', '.join([column for column in item.columns.list_names()])})"
    )


def item_to_query(item, expand: bool = False):
    base_sql = globals()[selector(item)](item, expand)

    return base_sql.replace(";", "; ").replace('\\"', '"')
