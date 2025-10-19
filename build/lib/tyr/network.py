import units
import itertools
import rustworkx as rx


def add_node(G, item):
    G.add_node(id(item))

    for key in item._node_data.keys():
        G.nodes(data=True)[id(item)][key] = item._node_data[key]

    return G


def add_edge(G, start, end, edge_data: dict = {}):
    G = add_edge(G, start, end)

    return G


def compose_all(graphs: list):
    return compose_all(graphs)


def has_path(G, start, end):
    return nx.has_path(G, start, end)


def shortest_path_length(G, start, end):
    return nx.shortest_path_length(G, start, end)


def all_simple_paths(G, start, end):
    return nx.all_simple_paths(G, start, end)


def dag_longest_path_length(G, start, end):
    return nx.dag_longest_path_length(G, start, end)


def source_column(item):
    G = rx.PyDiGraph()
    G = add_node(G, item)

    return G


def source_file(item):
    G = rx.PyDiGraph()
    G = add_node(G, item)

    return G


def core_column(item):
    G = rx.PyDiGraph()
    G = add_node(G, item)
    G = add_node(G, item.source)
    G = add_edge(G, item.source, item)
    G = compose_all([G, item_to_graph(item.source)])

    if "current_table" in dir(item):
        G = add_node(G, item.current_table)
        G = add_edge(G, item.current_table, item)

    elif "source_table" in dir(item):
        G = add_node(G, item.source_table)
        G = add_edge(G, item.source_table, item)

    return G


def column_wild_card(item):
    G = rx.PyDiGraph()
    G = add_node(G, item)
    G = add_node(G, item.current_table)
    G = add_edge(G, item.current_table, item)

    return G


def column_blank(item):
    G = rx.PyDiGraph()
    G = add_node(G, item)

    return G


def core_table(item):
    G = rx.PyDiGraph()
    G = add_node(G, item)

    if item.source:
        G = add_node(G, item.source)
        G = add_edge(G, item.source, item)
        G = compose_all([G, item_to_graph(item.source)])

    for column in item.columns.list_columns_():
        G = add_node(G, column)

        G.add_edge(
            id(item),
            id(column),
        )

        G = compose_all([G, item_to_graph(column)])

    if item.where_condition:
        G.add_node(
            id(item.where_condition),
            label="WHERE CONDITION",
            type=str(type(item.where_condition)),
            base=str(type(item.where_condition).__bases__[0]),
        )
        G = add_edge(G, item.where_condition, item)
        G = compose_all([G, item_to_graph(item.where_condition)])

    if (item.having_condition) and (item.group_by):
        G.add_node(
            id(item.having_condition),
            label="HAVING CONDITION",
            type=str(type(item.having_condition)),
            base=str(type(item.having_condition).__bases__[0]),
        )
        G = add_edge(G, item.having_condition, item)
        G = compose_all([G, item_to_graph(item.having_condition)])

    if not item.ctes.is_empty:
        for cte in item.ctes.list_tables_():
            G = add_node(G, cte)
            G = add_edge(G, cte, item)
            G = compose_all([G, item_to_graph(cte)])

    return G


def tables_subquery(item):
    G = rx.PyDiGraph()
    G = add_node(G, item)
    G = add_node(G, item.source)
    G = add_edge(G, item.source, item)
    G = compose_all([G, item_to_graph(item.source)])

    for column in item.columns.list_columns_():
        G = add_node(G, column)
        G = add_node(G, column.source)
        G = add_edge(G, column.source, column)
        G = compose_all([G, item_to_graph(column.source)])

    return G


def tables_from_records(item):
    G = rx.PyDiGraph()
    G = add_node(G, item)

    return G


def core_case_when(item):
    G = rx.PyDiGraph()
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
    G = add_edge(G, item.conditions[0], item.values[0])
    G = add_edge(G, item.conditions[0], item)

    for i in range(1, len(item.conditions)):
        G = add_node(G, item.conditions[i])
        G = add_node(G, item.values[i])

        G = add_edge(G, item.conditions[i], item)
        G = add_edge(G, item.conditions[i - 1], item.conditions[i])
        G = compose_all(
            [
                G,
                item_to_graph(item.conditions[i]),
                item_to_graph(item.conditions[i - 1]),
            ]
        )

    if item.else_value:
        G = add_node(G, item.else_value)

        G = add_edge(G, item.conditions[-1], item.else_value)
        G = add_edge(G, item.else_value, item)
        G = compose_all([G, item_to_graph(item.else_value)])

    return G


def core_condition(item):
    G = rx.PyDiGraph()
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

            G = add_edge(G, item.checks[i], item.link_operators[i])
            G = add_edge(G, item.checks[i + 1], item.link_operators[i])
            G = add_edge(G, item.link_operators[i], item)

            G = compose_all(
                [G, item_to_graph(item.checks[i]), item_to_graph(item.checks[i + 1])]
            )

    else:
        G = add_node(
            G,
            item.checks[0],
        )
        G = add_edge(G, item.checks[0], item)

        G = compose_all([G, item_to_graph(item.checks[0])])

    return G


def core_function(item):
    G = rx.PyDiGraph()
    G = add_node(G, item)

    for arg in item.args:
        G = add_node(G, arg)
        G = add_edge(G, arg, item)
        G = compose_all([G, item_to_graph(arg)])

    if not item.partition_by.is_empty:
        for column in item.partition_by.list_columns_():
            G = add_node(G, column)

            G = add_edge(
                G, column, item, edge_data={"type": str(type(item.partition_by))}
            )
            G = compose_all([G, item_to_graph(column)])

    if not item.order_by.columns.is_empty:
        for column in item.order_by.columns.list_columns_():
            G = add_node(G, column)

            G = add_edge(G, column, item, edge_data={"type": str(type(item.order_by))})
            G = compose_all([G, item_to_graph(column)])

    return G


def functions_source_wild_to_staging_column(item):
    G = rx.PyDiGraph()
    G = add_node(
        G,
        item,
    )
    G = add_node(G, item.args[0])
    G = add_edge(G, item.args[0], item)

    return G


def core_value(item):
    G = rx.PyDiGraph()

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
    G = rx.PyDiGraph()
    G = add_node(
        G,
        item,
    )

    for table in item.tables.list_tables_():
        G = add_node(G, table)

        G = add_edge(G, item, table)
        G = compose_all([G, item_to_graph(table)])

    return G


def joins_compound_join(item):
    G = rx.PyDiGraph()
    G = add_node(G, item)

    for join in item.joins:
        G = add_node(G, join)
        G = add_edge(G, join, item)
        G = compose_all([G, item_to_graph(join)])

    return G


def joins_join(item):
    G = rx.PyDiGraph()
    G = add_node(G, item)

    G = add_node(G, item.join_expression)
    G = add_edge(G, item.join_expression, item)
    G = compose_all([G, item_to_graph(item.join_expression)])

    G = add_node(G, item.condition)
    G = add_edge(G, item.condition, item)
    G = compose_all([G, item_to_graph(item.condition)])

    return G


def unions_union(item):
    G = rx.PyDiGraph()
    G = add_node(G, item)

    for table in item.source.list_columns_():
        G = add_node(G, table)
        G = add_edge(G, table, item)
        G = compose_all([G, item_to_graph(table)])

    for column in item.columns.list_columns_():
        G = add_node(G, column)
        G = add_edge(G, item, column)
        G = compose_all([G, item_to_graph(column)])

    return G


def unions_union_column(item):
    G = rx.PyDiGraph()
    G = add_node(G, item)

    for column in item.source.list_columns_():
        G = add_node(G, column)
        G = add_edge(G, column, item)
        G = compose_all([G, item_to_graph(column)])

    return G


def values_struct(item):
    G = rx.PyDiGraph()
    G = add_node(G, item)

    for value in item.values:
        G = add_node(G, value)
        G = add_edge(G, value, item)
        G = compose_all([G, item_to_graph(value)])

    return G


def values_interval(item):
    G = rx.PyDiGraph()
    G = add_node(G, item)

    return G


def values_subquery(item):
    G = rx.PyDiGraph()
    G = add_node(G, item)
    G = add_node(G, item.value)

    G = add_edge(G, item.value, item)

    return G


def values_list(item):
    G = rx.PyDiGraph()
    G = add_node(G, item)

    for i in range(len(item.value)):
        G = add_node(
            G,
            item.value[i],
        )
        G.add_edge(id(item.value[i]), id(item), index=i)
        G = compose_all([G, item_to_graph(item.value[i])])

    return G


def values_boolean(item):
    G = rx.PyDiGraph()
    G = add_node(G, item)

    return G


def core_quantity(item):
    G = rx.PyDiGraph()
    G = add_node(G, item)

    return G


# TRANSFORMATIONS
def transformations_limit(item):
    G = rx.PyDiGraph()
    G = add_node(G, item)

    G = add_node(G, item.source)
    G = add_edge(G, item.source, item)
    G = compose_all([G, item_to_graph(item.source)])

    return G


def core_transformation(item):
    G = rx.PyDiGraph()
    G = add_node(G, item)

    G = add_node(G, item.source)
    G = add_edge(G, item.source, item)
    G = compose_all([G, item_to_graph(item.source)])

    return G


# DATAFRAMES
def dataframes_data_frame(item):
    G = rx.PyDiGraph()
    G = add_node(G, item)
    G = add_node(G, item.source)
    G = add_edge(G, item.source, item)

    for column in item.columns.list_columns_():
        G = add_node(G, column)
        G = add_edge(G, item, column)

    return G


def dataframes_data_frame_column(item):
    G = rx.PyDiGraph()
    G = add_node(G, item)

    G = add_node(G, item.source)

    G = add_edge(G, item.source, item)

    return G


def dataframes_lambda_function(item):
    G = rx.PyDiGraph()
    G = add_node(G, item)

    for arg in item.args:
        G = add_node(G, arg)
        G = add_edge(G, arg, item)

    return G


# MACRO


def macros_macro(item):
    G = rx.PyDiGraph()
    add_node(G, item)
    G = compose_all([G, item_to_graph(item.macro)])
    G = add_edge(G, item, item.macro)

    for arg in item.args:
        G = compose_all([G, item_to_graph(arg)])
        G = add_edge(G, item, arg)

    return G


def item_to_graph(item):
    # SOURCE

    if "lineage.schema.source.SourceColumn" in str(type(item)):
        G = source_column(item)

    elif "lineage.schema.source.SourceFile" in str(type(item)):
        G = source_file(item)

    # COLUMNS

    elif "lineage.core._Column" in str(type(item).__bases__[0]):
        if "lineage.columns.Select" in str(type(item)):
            G = core_column(item)

        elif "lineage.columns.Core" in str(type(item)):
            G = core_column(item)

        elif "lineage.dataframes.LambdaOutput" in str(type(item)):
            G = dataframes_data_frame_column(item)

        elif "lineage.dataframes.DataFrameColumn" in str(type(item)):
            G = dataframes_data_frame_column(item)

        elif "lineage.columns.WildCard" in str(type(item)):
            G = column_wild_card(item)

    elif "lineage.columns.Blank" in str(type(item)):
        G = column_blank(item)

    # VALUES

    elif "lineage.core._Value" in str(type(item).__bases__[0]):
        if "lineage.values.Varchar" in str(type(item)):
            G = core_value(item)

        elif "lineage.values.Integer" in str(type(item)):
            G = core_value(item)

        elif "lineage.values.Float" in str(type(item)):
            G = core_value(item)

        elif "lineage.values.Timestamp" in str(type(item)):
            G = core_value(item)

        elif "lineage.values.WildCard" in str(type(item)):
            G = core_value(item)

        elif "lineage.values.Subquery" in str(type(item)):
            G = values_subquery(item)

        elif "lineage.values.Null" in str(type(item)):
            G = core_value(item)

        elif "lineage.values.Interval" in str(type(item)):
            G = values_interval(item)

        elif "lineage.values.List" in str(type(item)):
            G = values_list(item)

        elif "lineage.values.GeoCoordinate" in str(type(item)):
            G = values_list(item)

        elif "lineage.values.Datatype" in str(type(item)):
            G = core_value(item)

        elif "lineage.values.Tuple" in str(type(item)):
            G = core_value(item)

        elif "lineage.values.Struct" in str(type(item)):
            G = values_struct(item)

        elif "lineage.values.Boolean" in str(type(item)):
            G = values_boolean(item)

    # UNITS

    elif type(item) is units.core.Unit:
        G = rx.PyDiGraph()

    # TABLES

    elif "lineage.core._Table" in str(type(item).__bases__[0]):
        if "lineage.tables.Core" in str(type(item)):
            G = core_table(item)

        elif "lineage.tables.Select" in str(type(item)):
            G = core_table(item)

        elif "lineage.tables.Subquery" in str(type(item)):
            G = tables_subquery(item)

        elif "lineage.tables.Union" in str(type(item)):
            G = unions_union(item)

        elif "lineage.dataframes.DataFrame" in str(type(item)):
            G = dataframes_data_frame(item)

        elif "lineage.tables.FromRecords" in str(type(item)):
            G = tables_from_records(item)

    # FUNCTIONS

    elif "lineage.core._Function" in str(type(item).__bases__[0]):
        if not any(
            [
                x in str(type(item))
                for x in [
                    "lineage.functions.data_type.ToInterval",
                    "lineage.dataframes.LambdaFunction",
                    "lineage.functions.utility.SourceWildToStagingColumn",
                ]
            ]
        ):
            G = core_function(item)
        elif "lineage.functions.data_type.ToInterval" in str(type(item)):
            G = core_function(item)

        elif "lineage.dataframes.LambdaFunction" in str(type(item)):
            G = dataframes_lambda_function(item)

        elif "lineage.functions.utility.SourceWildToStagingColumn" in str(type(item)):
            G = functions_source_wild_to_staging_column(item)
        else:
            G = core_function(item)

    # EXPRESSIONS
    elif "lineage.core._Expression" in str(type(item).__bases__[0]):
        G = core_expression(item)

    # JOINS
    elif "lineage.joins.Join" in str(type(item)):
        G = joins_join(item)

    elif "lineage.joins.CompoundJoin" in str(type(item)):
        G = joins_compound_join(item)

    # CORE

    elif "lineage.core.CaseWhen" in str(type(item)):
        G = core_case_when(item)

    elif "lineage.core.Condition" in str(type(item)):
        G = core_condition(item)

    elif "lineage.transformations.Limit" in str(type(item)):
        G = transformations_limit(item)

    elif "lineage.transformations.ReadCSV" in str(type(item)):
        G = core_transformation(item)

    elif "lineage.transformations.ReadGeoJson" in str(type(item)):
        G = core_transformation(item)

    # elif "lineage.core.OrderBy" in str(type(item)):
    #
    #     G = core_order_by(item)
    #
    # elif "lineage.datamodels.DataModelSettings" in str(type(item).__bases__[0]):
    #
    #     G = datamodel_settings(item)

    elif "lineage.schema.core._Schema" in str(type(item).__bases__[0]):
        G = core_schema(item)

    else:
        raise ValueError("COULD NOT PARSE - ", str(type(item)))

    try:
        return G
    except:
        print(rf"FAILED ON {str(type(item))}")


class Spider:
    def __init__(self):
        self.rustworkx_version = rx.__version__

    def connect_schema(self, graph: rx.PyDiGraph):
        schema_nodes = [
            {"node": node} | data
            for node, data in graph.nodes(data=True)
            if data["base"] == "<class 'tyr.lineage.schema.core._Schema'>"
        ]

        schema_table_nodes = [
            {"node": node} | data
            for node, data in graph.nodes(data=True)
            if (data["type"] == "<class 'tyr.lineage.tables.Core'>")
            and (data["schema"] != "")
        ]

        schema_pairs = list(itertools.combinations(schema_nodes, 2))

        for pair in schema_pairs:
            s1 = pair[0]
            s2 = pair[1]

            s1_tables = [
                {"node": edge[1]} | graph.nodes[edge[1]]
                for edge in graph.out_edges(s1["node"])
            ]
            s2_tables = [
                {"node": edge[1]} | graph.nodes[edge[1]]
                for edge in graph.out_edges(s2["node"])
            ]

            for table in s1_tables:
                if has_path(graph, s1["node"], table["node"]) and has_path(
                    graph, s2["node"], table["node"]
                ):
                    if shortest_path_length(
                        graph, s1["node"], table["node"]
                    ) > shortest_path_length(graph, s2["node"], table["node"]):
                        graph.add_edge(s1["node"], s2["node"])
                    else:
                        graph.add_edge(s2["node"], s1["node"])

            for table in s2_tables:
                if has_path(graph, s1["node"], table["node"]) and has_path(
                    graph, s2["node"], table["node"]
                ):
                    if shortest_path_length(
                        graph, s1["node"], table["node"]
                    ) > shortest_path_length(graph, s2["node"], table["node"]):
                        graph.add_edge(s1["node"], s2["node"])
                    else:
                        graph.add_edge(s2["node"], s1["node"])

        for pair in schema_pairs:
            s1 = pair[0]
            s2 = pair[1]

            if len(list(all_simple_paths(graph, s1["node"], s2["node"]))) > 1:
                if dag_longest_path_length(graph, s1["node"], s2["node"]) > 1:
                    graph.remove_edge(s1["node"], s2["node"])
            elif len(list(all_simple_paths(graph, s2["node"], s1["node"]))) > 1:
                if dag_longest_path_length(graph, s2["node"], s1["node"]) > 1:
                    graph.remove_edge(s2["node"], s1["node"])
            else:
                pass

        return graph

    def item_to_graph(self, item):
        return item_to_graph(item)
