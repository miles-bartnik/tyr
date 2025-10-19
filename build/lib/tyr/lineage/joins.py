from ..lineage import core as lineage
from ..lineage import columns as lineage_columns
from typing import List
import rustworkx as rx
import sqlparse


class Join:

    """
    Join object

    :param join_expression: Join expression
    :type join_expression: lineage._Expression
    :param condition: Condition to join on
    :type condition: lineage.Condition
    :param condition: Condition to join on
    :type condition: lineage.Condition
    :param macro_group: Used to group multiple pre-fabricated lineage objects into the same custom node collection - default value [""]
    :type macro_group: str
    """

    def __init__(
        self,
        join_expression: lineage._Expression,
        condition: lineage.Condition,
        macro_group: str = "",
    ) -> None:
        self.join_expression = join_expression
        self.condition = condition
        self.name = rf"JOIN EXPRESSION - {id(self)}"
        self.primary_key = lineage.ColumnList([])
        self.event_time = None

        left_columns = lineage.ColumnList(
            [
                lineage_columns.Select(column)
                for column in self.join_expression.left.columns.list_columns_()
            ]
        )

        right_columns = lineage.ColumnList(
            [
                lineage_columns.Select(column)
                for column in self.join_expression.right.columns.list_columns_(
                    filter_regex=rf"^(?!{'|'.join([column for column in left_columns.list_names_()])}).*"
                )
            ]
        )

        self.columns = left_columns + right_columns

        left_ctes = self.join_expression.left.ctes.list_tables_()
        right_ctes = self.join_expression.right.ctes.list_tables_()

        self.ctes = lineage.TableList(left_ctes + right_ctes)
        self.sql = sqlparse.format(
            lineage._interpreters["beeswax_duckdb"].to_sql(self), reindent=True
        )
        self._node_data = {
            "type": str(type(self)),
            "base": str(type(self)),
            "macro_group": macro_group,
            "sql": self.sql,
        }

        left = rx.PyDiGraph()
        right = rx.PyDiGraph()

        left.add_node(self.join_expression.left._node_data)
        right.add_node(self.join_expression.right._node_data)

        left.add_child(0, self._node_data, {})
        right.add_child(0, self._node_data, {})

        left = rx.union(
            left,
            self.join_expression.left.graph.rx_graph,
            merge_nodes=True,
            merge_edges=True,
        )
        right = rx.union(
            right,
            self.join_expression.right.graph.rx_graph,
            merge_nodes=True,
            merge_edges=True,
        )

        graph = rx.union(left, right, merge_nodes=True, merge_edges=True)
        self.graph = lineage.LineageGraph(rx_graph=graph)


class CompoundJoin:

    """
    CompounJoin object which chains multiple joins together

    :param joins: List of joins to combine
    :type joins: List[Join]
    :param macro_group: Used to group multiple pre-fabricated lineage objects into the same custom node collection - default value [""]
    :type macro_group: str
    """

    def __init__(self, joins: List[Join], macro_group: str = ""):
        self.name = rf"COMPOUND JOIN - {id(self)}"
        self.joins = joins

        self.columns = lineage.ColumnList([])
        self.ctes = lineage.TableList([])
        self.primary_key = lineage.ColumnList([])
        self.event_time = None

        for join in joins:
            if not any(
                [
                    column.current_table.name == column_name
                    for column_name in self.columns.list_names_()
                    for column in join.columns.list_columns_()
                ]
            ):
                for column in join.columns.list_columns_():
                    if column.name not in self.columns.list_names_():
                        self.columns.add_(column)

        for join in joins:
            for cte in join.ctes.list_tables_():
                if cte.name not in self.ctes.list_names_():
                    self.ctes.add_(cte)
        self.sql = sqlparse.format(
            lineage._interpreters["beeswax_duckdb"].to_sql(self), reindent=True
        )
        self._node_data = {
            "type": str(type(self)),
            "base": str(type(self)),
            "macro_group": macro_group,
            "sql": self.sql,
        }

        graph = rx.PyDiGraph()

        for join in self.joins:
            graph = rx.union(
                graph, join.graph.rx_graph, merge_nodes=True, merge_edges=True
            )

        self.graph = lineage.LineageGraph(rx_graph=graph)
