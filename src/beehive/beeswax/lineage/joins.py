from ..lineage import core as lineage
from ..lineage import columns as lineage_columns
from ..lineage import operators as lineage_operators
from ..lineage import expressions as lineage_expressions
from typing import List
import networkx as nx


class Union:
    def __init__(
        self,
        tables: lineage.TableList,
        columns: lineage.ColumnList = lineage.ColumnList([]),
        primary_key: lineage.ColumnList = lineage.ColumnList([]),
        distinct: bool = False,
    ) -> None:
        self.columns = columns
        self.tables = tables
        self.ctes = lineage.TableList([])

        for table in self.tables.list_all():
            for cte in table.ctes.list_all():
                if not cte.name in self.ctes.list_names():
                    self.ctes.add(cte)

        if self.columns.is_empty:
            columns = lineage.ColumnList(
                [
                    lineage_columns.Select(column)
                    for column in lineage.ColumnList(
                        self.tables.list_all()[0].columns[
                            list(
                                set.intersection(
                                    *map(
                                        set,
                                        [
                                            table.columns.list_names()
                                            for table in tables.list_all()
                                        ],
                                    )
                                )
                            )
                        ]
                    ).list_all()
                ]
            )
        else:
            columns = lineage.ColumnList(
                [
                    lineage_columns.Select(column)
                    for column in lineage.ColumnList(
                        self.tables.list_all()[0].columns[
                            list(
                                set.intersection(
                                    *map(
                                        set,
                                        [
                                            [
                                                column.name
                                                for column in table.columns[
                                                    self.columns.list_names()
                                                ]
                                            ]
                                            for table in tables.list_all()
                                        ],
                                    )
                                )
                            )
                        ]
                    )
                ]
            )

        for column in columns.list_all():
            for table in tables.list_all():
                if column.name in table.columns.list_names():
                    if (
                        column.data_type.name
                        == table.columns[column.name].data_type.name
                    ):
                        pass

                    else:
                        raise ValueError(rf"""{column.name} has mixed datatypes""")

        self.columns = columns

        self.distinct = distinct

        self.name = rf"SELECT {', '.join(self.columns.list_names())} FROM {' UNION '.join(self.tables.list_names())}"

    def _node_data(self):
        return {"type": str(type(self))}

    def _outbound_edge_data(self):
        return {}

    def _inbound_edge_data(self):
        return {}


class Join:
    def __init__(
        self,
        join_expression: lineage._Expression,
        condition: lineage.Condition,
    ) -> None:
        self.join_expression = join_expression
        self.condition = condition
        self.name = rf"JOIN EXPRESSION - {id(self)}"

        left_columns = lineage.ColumnList(
            [
                lineage_columns.Select(column)
                for column in self.join_expression.left.columns.list_all()
            ]
        )
        right_columns = lineage.ColumnList(
            [
                lineage_columns.Select(column)
                for column in self.join_expression.right.columns.list_all()
            ]
        )

        self.columns = lineage.ColumnList(
            left_columns.list_all() + right_columns.list_all()
        )

        left_ctes = self.join_expression.left.ctes.list_all()
        right_ctes = self.join_expression.right.ctes.list_all()

        self.ctes = lineage.TableList(left_ctes + right_ctes)

    def _node_data(self):
        return {"type": str(type(self))}

    def _outbound_edge_data(self):
        return {}

    def _inbound_edge_data(self):
        return {}


class JoinList:
    def __init__(self, joins: List[Join]):
        self.name = rf"JOIN LIST - {id(self)}"
        self.joins = joins

        self.columns = lineage.ColumnList([])
        self.ctes = lineage.TableList([])

        for join in joins:
            if not any(
                [
                    column.current_table.name == column_name
                    for column_name in self.columns.list_names()
                    for column in join.columns.list_all()
                ]
            ):
                for column in join.columns.list_all():
                    if column.name not in self.columns.list_names():
                        self.columns.add(column)

        for join in joins:
            for cte in join.ctes.list_all():
                if cte.name not in self.ctes.list_names():
                    self.ctes.add(cte)

    def _node_data(self):
        return {"type": str(type(self))}

    def _outbound_edge_data(self):
        return {}

    def _inbound_edge_data(self):
        return {}
