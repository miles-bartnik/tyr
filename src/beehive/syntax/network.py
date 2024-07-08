import networkx as nx
import re
from ..beeswax import lineage
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

    G = add_node(G, item.current_table)

    G.add_edge(id(item.current_table), id(item))

    G = nx.compose_all([G, item_to_graph(item.source)])

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

        G = nx.compose_all([G] + [item_to_graph(column)])

    if item.where_condition:
        G.add_node(
            id(item.where_condition),
            label="WHERE CONDITION",
            type=str(type(item.where_condition)),
            base=str(type(item.where_condition).__bases__[0]),
        )
        G.add_edge(id(item.where_condition), id(item))
        G = nx.compose_all([G, item_to_graph(item.where_condition)])

    if (item.having_condition) and (item.group_by):
        G.add_node(
            id(item.having_condition),
            label="HAVING CONDITION",
            type=str(type(item.having_condition)),
            base=str(type(item.having_condition).__bases__[0]),
        )
        G.add_edge(id(item.having_condition), id(item))
        G = nx.compose_all([G, item_to_graph(item.having_condition)])

    if item.source:
        G = nx.compose_all([G, item_to_graph(item.source)])

    if not item.ctes.is_empty:
        for cte in item.ctes.list_all():
            G = add_node(G, cte)
            G.add_edge(id(cte), id(item))
            G = nx.compose_all([G, item_to_graph(cte)])

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

    G = nx.compose_all(
        [G, item_to_graph(item.conditions[0]), item_to_graph(item.values[0])]
    )

    for i in range(1, len(item.conditions)):
        G = add_node(G, item.conditions[i])
        G = add_node(G, item.values[i])

        G.add_edge(id(item.conditions[i]), id(item))
        G.add_edge(id(item.conditions[i - 1]), id(item.conditions[i]))

        G = nx.compose_all(
            [G, item_to_graph(item.conditions[i]), item_to_graph(item.values[i])]
        )

    if item.else_value:
        G = add_node(G, item.else_value)

        G.add_edge(id(item.conditions[-1]), id(item.else_value))
        G.add_edge(id(item.else_value), id(item))

        G = nx.compose_all([G, item_to_graph(item.else_value)])

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

    G = nx.compose_all([G] + [item_to_graph(check) for check in item.checks])

    return G


def core_function(item):
    G = nx.DiGraph()
    G = add_node(G, item)

    for arg in item.args:
        G = add_node(G, arg)
        G.add_edge(id(arg), id(item))

        G = nx.compose_all([G, item_to_graph(arg)])

    if not item.partition_by.is_empty:
        for column in item.partition_by.list_all():
            G = add_node(G, column)

            G.add_edge(id(column), id(item), type=str(type(item.partition_by)))

            G = nx.compose_all([G, item_to_graph(column)])

    if not item.order_by.columns.is_empty:
        for column in item.order_by.columns.list_all():
            G = add_node(G, column)

            G.add_edge(id(column), id(item), type=str(type(item.order_by)))

            G = nx.compose_all([G, item_to_graph(column)])

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

    G = nx.compose_all([G, item_to_graph(item.left), item_to_graph(item.right)])

    return G


def core_aggregate(item):
    G = nx.DiGraph()
    G.add_node(id(item), type=str(type(item)), base=str(type(item).__bases__[0]))

    for i in range(len(item.args)):
        G = add_node(
            G,
            item.args[i],
        )
        G.add_edge(id(item.args[i]), id(item))

        G = nx.compose_all([G, item_to_graph(item.args[i])])

    if not item.partition_by.is_empty:
        for column in item.partition_by.list_all():
            G = add_node(
                G,
                column,
            )

            G.add_edge(id(column), id(item), type=str(type(item.partition_by)))

            G = nx.compose_all([G, item_to_graph(column)])

    if item.order_by:
        for column in item.order_by.columns.list_all():
            G = add_node(
                G,
                column,
            )

            G.add_edge(id(column), id(item), type=str(type(item.order_by)))

            G = nx.compose_all([G, item_to_graph(column)])

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

        G = nx.compose_all([G, item_to_graph(table)])

    return G


def combinations_join_list(item):
    G = nx.DiGraph()
    G = add_node(G, item)

    for join in item.joins:
        G = add_node(G, join)
        G.add_edge(id(join), id(item))
        G = nx.compose_all([G, item_to_graph(join)])

    return G


def combinations_join(item):
    G = nx.DiGraph()
    G = add_node(G, item)

    G = add_node(G, item.join_expression)
    G.add_edge(id(item.join_expression), id(item))

    G = add_node(G, item.condition)
    G.add_edge(id(item.condition), id(item))

    G = nx.compose_all(
        [G, item_to_graph(item.join_expression), item_to_graph(item.condition)]
    )

    return G


def combinations_union(item):
    G = nx.DiGraph()
    G = add_node(G, item)

    for table in item.tables.list_all():
        G = add_node(G, table)
        G.add_edge(id(table), id(item))

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
    G = nx.compose_all([G, item_to_graph(item.value)])

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


def core_quantity(item):
    G = nx.DiGraph()
    G = add_node(G, item)

    return G


def item_to_graph(item):
    # SOURCE

    if type(item).__bases__[0] is lineage.core._SourceColumn:
        G = source_column(item)

    elif type(item).__bases__[0] is lineage.core._SourceFile:
        G = source_file(item)

    # COLUMNS

    elif type(item).__bases__[0] is lineage.core._Column:
        if type(item) is lineage.columns.Select:
            G = core_column(item)

        elif type(item) is lineage.columns.Expand:
            G = core_column(item)

    # VALUES

    elif type(item).__bases__[0] is lineage.core._Value:
        if type(item) is lineage.values.Varchar:
            G = core_value(item)

        elif type(item) is lineage.values.Integer:
            G = core_value(item)

        elif type(item) is lineage.values.Float:
            G = core_value(item)

        elif type(item) is lineage.values.Timestamp:
            G = core_value(item)

        elif type(item) is lineage.values.WildCard:
            G = core_value(item)

        elif type(item) is lineage.values.Subquery:
            G = values_subquery(item)

        elif type(item) is lineage.values.Null:
            G = core_value(item)

        elif type(item) is lineage.values.Interval:
            G = values_interval(item)

        elif type(item) is lineage.values.Limit:
            G = core_value(item)

        elif type(item) is lineage.values.List:
            G = values_list(item)

        elif type(item) is lineage.values.GeoCoordinate:
            G = values_list(item)

        elif type(item) is lineage.values.Datatype:
            G = core_value(item)

        elif type(item) is lineage.values.Tuple:
            G = core_value(item)

        elif type(item) is lineage.values.Struct:
            G = values_struct(item)

    # UNITS

    elif type(item) is units.core.Unit:
        G = nx.DiGraph()

    # TABLES

    elif type(item).__bases__[0] is lineage.core._Table:
        if type(item) is lineage.tables.Core:
            G = core_table(item)

        elif type(item) is lineage.tables.Select:
            G = core_table(item)

        elif type(item) is lineage.tables.Subquery:
            G = core_table(item)

    # FUNCTIONS

    elif type(item).__bases__[0] is lineage.core._Function:
        if type(item) not in [
            lineage.functions.ToInterval,
        ]:
            G = core_function(item)
        elif type(item) is lineage.functions.ToInterval:
            G = core_function(item)

    # AGGREGATES

    elif type(item).__bases__[0] is lineage.core._Aggregate:
        G = core_aggregate(item)

    # EXPRESSIONS
    elif type(item).__bases__[0] is lineage.core._Expression:
        G = core_expression(item)

    # COMBINATIONS
    elif type(item) is lineage.combinations.Join:
        G = combinations_join(item)

    elif type(item) is lineage.combinations.JoinList:
        G = combinations_join_list(item)

    elif type(item) is lineage.combinations.Union:
        G = combinations_union(item)

    # CORE

    elif type(item) is lineage.core.CaseWhen:
        G = core_case_when(item)

    elif type(item) is lineage.core.Condition:
        G = core_condition(item)

    # elif type(item) is lineage.core.OrderBy:
    #
    #     G = core_order_by(item)
    #
    # elif type(item).__bases__[0] is lineage.datamodels.DataModelSettings:
    #
    #     G = datamodel_settings(item)

    else:
        raise ValueError("COULD NOT PARSE - ", type(item))

    return G
