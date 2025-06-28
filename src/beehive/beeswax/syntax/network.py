import networkx as nx
import units


def add_node(G, item):
    G.add_node(id(item))

    for key in item._node_data().keys():
        G.nodes(data=True)[id(item)][key] = item._node_data()[key]

    return G


def source_column(item):
    G = nx.DiGraph()
    G = add_node(G, item)

    return G


def source_file(item):
    G = nx.DiGraph()
    G = add_node(G, item)

    for column in item.columns.list_all():
        G = add_node(G, column)
        G.add_edge(id(column), id(item))

    return G


def core_column(item):
    G = nx.DiGraph()
    G = add_node(G, item)
    G = add_node(G, item.source)
    G.add_edge(id(item.source), id(item))

    if "current_table" in dir(item):
        G = add_node(G, item.current_table)
        G.add_edge(id(item.current_table), id(item))

    elif "source_table" in dir(item):
        G = add_node(G, item.source_table)
        G.add_edge(id(item.source_table), id(item))

    return G


def column_blank(item):
    G = nx.DiGraph()
    G = add_node(G, item)

    return G


def core_table(item):
    G = nx.DiGraph()
    G = add_node(G, item)

    if item.source:
        G = add_node(G, item.source)
        G.add_edge(id(item.source), id(item))

    for column in item.columns.list_all():
        G = add_node(G, column)

        G.add_edge(
            id(item),
            id(column),
        )

    if item.where_condition:
        G.add_node(
            id(item.where_condition),
            label="WHERE CONDITION",
            type=str(type(item.where_condition)),
            base=str(type(item.where_condition).__bases__[0]),
        )
        G.add_edge(id(item.where_condition), id(item))

    if (item.having_condition) and (item.group_by):
        G.add_node(
            id(item.having_condition),
            label="HAVING CONDITION",
            type=str(type(item.having_condition)),
            base=str(type(item.having_condition).__bases__[0]),
        )
        G.add_edge(id(item.having_condition), id(item))

    if not item.ctes.is_empty:
        for cte in item.ctes.list_all():
            G = add_node(G, cte)
            G.add_edge(id(cte), id(item))

    return G


def tables_subquery(item):
    G = nx.DiGraph()
    G = add_node(G, item)
    G = add_node(G, item.source)
    G.add_edge(id(item.source), id(item))

    for column in item.columns.list_all():
        G = add_node(G, column)
        G = add_node(G, column.source)
        G.add_edge(id(column.source), id(column))

    return G


def tables_from_records(item):
    G = nx.DiGraph()
    G = add_node(G, item)

    return G


def core_case_when(item):
    G = nx.DiGraph()
    G = add_node(G, item)
    # G.add_node(
    #     id(item),
    #     label="CASE WHEN RESULT",
    #     type=str(type(item)),
    #     base=str(type(item).__bases__[0]),
    # )
    #
    G = add_node(G, item.conditions[0])
    # G.add_node(
    #     id(item.conditions[0]),
    #     type=str(type(item.conditions[0])),
    #     base=str(type(item.conditions[0]).__bases__[0]),
    # )
    G = add_node(G, item.values[0])
    # G.add_node(
    #     id(item.values[0]),
    #     type=str(type(item.values[0])),
    #     base=str(type(item.values[0]).__bases__[0]),
    # )

    # for key, value in globals()[rf"{value_module}_{value_class}_node_data"](
    #     item.values[0]
    # ).items():
    #     G.nodes[(id(item.values[0]))][key] = value
    G.add_edge(id(item.conditions[0]), id(item.values[0]))
    G.add_edge(id(item.conditions[0]), id(item))

    for i in range(1, len(item.conditions)):
        G = add_node(G, item.conditions[i])
        G = add_node(G, item.values[i])

        G.add_edge(id(item.conditions[i]), id(item))
        G.add_edge(id(item.conditions[i - 1]), id(item.conditions[i]))

    if item.else_value:
        G = add_node(G, item.else_value)

        G.add_edge(id(item.conditions[-1]), id(item.else_value))
        G.add_edge(id(item.else_value), id(item))

    return G


def core_condition(item):
    G = nx.DiGraph()
    G = add_node(
        G,
        item,
    )

    if item.link_operators:
        for i in range(len(item.checks) - 1):
            G = add_node(
                G,
                item.checks[i],
            )
            G = add_node(
                G,
                item.checks[i + 1],
            )
            G = add_node(
                G,
                item.link_operators[i],
            )

            G.add_edge(id(item.checks[i]), id(item.link_operators[i]))
            G.add_edge(id(item.checks[i + 1]), id(item.link_operators[i]))
            G.add_edge(id(item.link_operators[i]), id(item))

    else:
        G = add_node(
            G,
            item.checks[0],
        )
        G.add_edge(id(item.checks[0]), id(item))

    return G


def core_function(item):
    G = nx.DiGraph()
    G = add_node(G, item)

    for arg in item.args:
        G = add_node(G, arg)
        G.add_edge(id(arg), id(item))

    if not item.partition_by.is_empty:
        for column in item.partition_by.list_all():
            G = add_node(G, column)

            G.add_edge(id(column), id(item), type=str(type(item.partition_by)))

    if not item.order_by.columns.is_empty:
        for column in item.order_by.columns.list_all():
            G = add_node(G, column)

            G.add_edge(id(column), id(item), type=str(type(item.order_by)))

    return G


def core_expression(item):
    G = nx.DiGraph()
    G = add_node(
        G,
        item,
    )

    G = add_node(
        G,
        item.left,
    )
    G = add_node(
        G,
        item.right,
    )

    G.add_edge(id(item.left), id(item))
    G.add_edge(id(item.right), id(item))

    return G


def core_value(item):
    G = nx.DiGraph()

    if not item.data_type:
        data_type = ""
    elif type(item.data_type) is str:
        data_type = item.data_type
    else:
        data_type = item.data_type.value

    if not item.value:
        value = "NULL"
    else:
        value = item.value

    G = add_node(
        G,
        item,
    )

    return G


def core_schema(item):
    G = nx.DiGraph()
    G = add_node(
        G,
        item,
    )

    for table in item.tables.list_all():
        G = add_node(G, table)

        G.add_edge(id(item), id(table))

    return G


def joins_compound_join(item):
    G = nx.DiGraph()
    G = add_node(G, item)

    for join in item.joins:
        G = add_node(G, join)
        G.add_edge(id(join), id(item))

    return G


def joins_join(item):
    G = nx.DiGraph()
    G = add_node(G, item)

    G = add_node(G, item.join_expression)
    G.add_edge(id(item.join_expression), id(item))

    G = add_node(G, item.condition)
    G.add_edge(id(item.condition), id(item))

    return G


def unions_union(item):
    G = nx.DiGraph()
    G = add_node(G, item)

    for table in item.source.list_all():
        G = add_node(G, table)
        G.add_edge(id(table), id(item))

    for column in item.columns.list_all():
        G = add_node(G, column)
        G.add_edge(id(item), id(column))

    return G


def unions_union_column(item):
    G = nx.DiGraph()
    G = add_node(G, item)

    for column in item.source.list_all():
        G = add_node(G, column)
        G.add_edge(id(column), id(item))

    return G


def values_struct(item):
    G = nx.DiGraph()
    G = add_node(G, item)

    for value in item.values:
        G = add_node(G, value)
        G.add_edge(id(value), id(item))

    return G


def values_interval(item):
    G = nx.DiGraph()
    G = add_node(G, item)

    return G


def values_subquery(item):
    G = nx.DiGraph()
    G = add_node(G, item)
    G = add_node(G, item.value)

    G.add_edge(id(item.value), id(item))

    return G


def values_list(item):
    G = nx.DiGraph()
    G = add_node(G, item)

    for i in range(len(item.value)):
        G = add_node(
            G,
            item.value[i],
        )
        G.add_edge(id(item.value[i]), id(item), index=i)

    return G


def values_boolean(item):
    G = nx.DiGraph()
    G = add_node(G, item)

    return G


def core_quantity(item):
    G = nx.DiGraph()
    G = add_node(G, item)

    return G


# TRANSFORMATIONS
def transformations_limit(item):
    G = nx.DiGraph()
    G = add_node(G, item)

    G = add_node(G, item.source)
    G.add_edge(id(item.source), id(item))

    return G


def core_transformation(item):
    G = nx.DiGraph()
    G = add_node(G, item)

    G = add_node(G, item.source)
    G.add_edge(id(item.source), id(item))

    return G


# DATAFRAMES
def dataframes_data_frame(item):
    G = nx.DiGraph()
    G = add_node(G, item)
    G = add_node(G, item.source)
    G.add_edge(id(item.source), id(item))

    for column in item.columns.list_all():
        G = add_node(G, column)
        G.add_edge(id(item), id(column))

    return G


def dataframes_data_frame_column(item):
    G = nx.DiGraph()
    G = add_node(G, item)

    G = add_node(G, item.source)

    G.add_edge(id(item.source), id(item))

    return G


def dataframes_lambda_function(item):
    G = nx.DiGraph()
    G = add_node(G, item)

    for arg in item.args:
        G = add_node(G, arg)
        G.add_edge(id(arg), id(item))

    return G


# MACRO


def macros_macro(item):
    G = nx.DiGraph()


def item_to_graph(item):
    # MACRO
    if "lineage.macros.core.Macro" in type(item).__bases__[0]:
        G = macros_macro(item)

    # SOURCE

    elif "lineage.core._SourceColumn" in type(item).__bases__[0]:
        G = source_column(item)

    elif "lineage.core._SourceFile" in type(item).__bases__[0]:
        G = source_file(item)

    # COLUMNS

    elif "lineage.core._Column" in type(item).__bases__[0]:
        if "lineage.columns.Select" in type(item):
            G = core_column(item)

        elif "lineage.columns.Core" in type(item):
            G = core_column(item)

        elif "lineage.dataframes.LambdaOutput" in type(item):
            G = dataframes_data_frame_column(item)

        elif "lineage.dataframes.DataFrameColumn" in type(item):
            G = dataframes_data_frame_column(item)

    elif "lineage.columns.Blank" in type(item):
        G = column_blank(item)

    # VALUES

    elif "lineage.core._Value" in type(item).__bases__[0]:
        if "lineage.values.Varchar" in type(item):
            G = core_value(item)

        elif "lineage.values.Integer" in type(item):
            G = core_value(item)

        elif "lineage.values.Float" in type(item):
            G = core_value(item)

        elif "lineage.values.Timestamp" in type(item):
            G = core_value(item)

        elif "lineage.values.WildCard" in type(item):
            G = core_value(item)

        elif "lineage.values.Subquery" in type(item):
            G = values_subquery(item)

        elif "lineage.values.Null" in type(item):
            G = core_value(item)

        elif "lineage.values.Interval" in type(item):
            G = values_interval(item)

        elif "lineage.values.List" in type(item):
            G = values_list(item)

        elif "lineage.values.GeoCoordinate" in type(item):
            G = values_list(item)

        elif "lineage.values.Datatype" in type(item):
            G = core_value(item)

        elif "lineage.values.Tuple" in type(item):
            G = core_value(item)

        elif "lineage.values.Struct" in type(item):
            G = values_struct(item)

        elif "lineage.values.Boolean" in type(item):
            G = values_boolean(item)

    # UNITS

    elif type(item) is units.core.Unit:
        G = nx.DiGraph()

    # TABLES

    elif "lineage.core._Table" in type(item).__bases__[0]:
        if "lineage.tables.Core" in type(item):
            G = core_table(item)

        elif "lineage.tables.Select" in type(item):
            G = core_table(item)

        elif "lineage.tables.Subquery" in type(item):
            G = tables_subquery(item)

        elif "lineage.tables.Union" in type(item):
            G = unions_union(item)

        elif "lineage.dataframes.DataFrame" in type(item):
            G = dataframes_data_frame(item)

        elif "lineage.tables.FromRecords" in type(item):
            G = tables_from_records(item)

    # FUNCTIONS

    elif "lineage.core._Function" in type(item).__bases__[0]:
        if type(item) not in [
            lineage.functions.data_type.ToInterval,
            lineage.dataframes.LambdaFunction,
        ]:
            G = core_function(item)
        elif "lineage.functions.data_type.ToInterval" in type(item):
            G = core_function(item)

        elif "lineage.dataframes.LambdaFunction" in type(item):
            G = dataframes_lambda_function(item)

    # EXPRESSIONS
    elif "lineage.core._Expression" in type(item).__bases__[0]:
        G = core_expression(item)

    # JOINS
    elif "lineage.joins.Join" in type(item):
        G = joins_join(item)

    elif "lineage.joins.CompoundJoin" in type(item):
        G = joins_compound_join(item)

    # CORE

    elif "lineage.core.CaseWhen" in type(item):
        G = core_case_when(item)

    elif "lineage.core.Condition" in type(item):
        G = core_condition(item)

    elif "lineage.transformations.Limit" in type(item):
        G = transformations_limit(item)

    elif "lineage.transformations.ReadCSV" in type(item):
        G = core_transformation(item)

    elif "lineage.transformations.ReadGeoJson" in type(item):
        G = core_transformation(item)

    # elif "lineage.core.OrderBy" in type(item):
    #
    #     G = core_order_by(item)
    #
    # elif "lineage.datamodels.DataModelSettings" in type(item).__bases__[0]:
    #
    #     G = datamodel_settings(item)

    elif "lineage.schema.Schema" in type(item):
        G = core_schema(item)

    else:
        raise ValueError("COULD NOT PARSE - ", type(item))

    try:
        return G
    except:
        print(rf"FAILED ON {str(type(item))}")
