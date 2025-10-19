from ..lineage import core as lineage
from ..lineage import columns as lineage_columns
from ..lineage import functions as lineage_functions


class Core(lineage._Table):

    """
    Core Table object that is the main table type required for most internal applications (Schema, TableList, etc.)

    :param name: Name of table object
    :type name: str
    :param columns: List of _Column objects belonging to table
    :type columns: lineage.ColumnList
    :param source: Source of table object. This can be ``None``. Default: ``None``,
    :type source: lineage._Table|lineage.joins.Join|lineage.joins.CompoundJoin|lineage.transformations.Union
    :param inherit_primary_key: Inherit primary key as Select objects from source table. Overrides primary_key. Default: ``False``
    :type inherit_primary_key: bool
    :param primary_key: List of columns to use as primary key. Default: ``lineage.ColumnList([])``
    :type primary_key: lineage.ColumnList
    :param inherit_event_time: Inherit event time as Select object from source table. Overrides event_time. Default: ``False``
    :type inherit_event_time: bool
    :param event_time: Column to use as event time. Default: ``None``
    :type event_time: lineage._Column
    :param distinct: Whether the table query only selects distinct rows. Default: ``False``
    :type distinct: bool
    :param group_by: Whether to group by the primary key. Default: ``False``
    :type group_by: bool
    :param where_condition: Condition to refine row selection. Default: ``None``
    :type where_condition: lineage.Condition
    :param having_condition: Condition to refine row selection for group_by. Default: ``None``
    :type having_condition: lineage.Condition
    :param order_by: OrderBy to order results. Default: ``None``
    :type order_by: lineage.OrderBy
    :param ctes: Common Table Expressions (CTE) to be created in a WITH statement preceeding the query. Must be in correct order. Default: ``None``
    :type ctes: lineage.TableList
    :param macro_group: Used to group multiple pre-fabricated lineage objects into the same custom node collection - Default: ``""``
    :type macro_group: str
    """

    def __init__(
        self,
        name,
        columns,
        source=None,
        inherit_primary_key: bool = False,
        primary_key: lineage.ColumnList = lineage.ColumnList([]),
        inherit_event_time: bool = False,
        event_time=None,
        distinct: bool = False,
        group_by: bool = False,
        where_condition: lineage.Condition = None,
        having_condition: lineage.Condition = None,
        order_by: lineage.OrderBy = None,
        ctes=None,
        macro_group: str = "",
    ) -> None:
        if not all(
            [isinstance(column, lineage._Column) for column in columns.list_columns_()]
        ):
            print(
                [
                    rf"{column.name} - {type(column)}"
                    for column in columns.list_columns_()
                ]
            )
            raise ValueError("All columns must be Core, Blank, or WildCard")

        if ctes:
            for table in ctes.list_tables_():
                if type(table) not in [Core, Select]:
                    print(table.__dict__)
                    raise ValueError("cte must be either Core or Select")

        if (inherit_primary_key) and not (primary_key.is_empty):
            raise ValueError(
                "inherit_primary_key=True contradicted by primary_key.is_empty=False"
            )
        elif inherit_primary_key:
            primary_key = lineage.ColumnList(
                [
                    lineage_columns.Select(column)
                    for column in source.primary_key.list_columns_()
                ]
            )

        if (inherit_event_time) and (event_time):
            raise ValueError(
                "inherit_event_time=True contradicted by event_time not None"
            )
        elif inherit_event_time and (source.event_time):
            event_time = lineage_columns.Select(source.event_time)

        super().__init__(
            name=name,
            columns=columns,
            source=source,
            primary_key=primary_key,
            distinct=distinct,
            group_by=group_by,
            event_time=event_time,
            where_condition=where_condition,
            having_condition=having_condition,
            ctes=ctes,
            macro_group=macro_group,
            order_by=order_by,
        )

        self._node_data = self._node_data | {"schema": ""}

    def __deepcopy__(self, memodict={}):
        return Core(
            name=self.name,
            columns=self.columns,
            source=self.source,
            primary_key=self.primary_key,
            distinct=self.distinct,
            group_by=self.group_by,
            event_time=self.event_time,
            where_condition=self.where_condition,
            having_condition=self.having_condition,
            ctes=self.ctes,
            order_by=self.order_by,
        )


class Select(lineage._Table):
    def __init__(self, source, alias: str = None, macro_group: str = "") -> None:
        if isinstance(source, lineage._Table):
            if alias:
                name = alias
            else:
                name = source.name

        else:
            raise ValueError(rf"Invalid source - {type(source)}")

        if source.event_time:
            super().__init__(
                name=name,
                columns=lineage.ColumnList(
                    [
                        lineage_columns.Select(column)
                        for column in source.columns.list_columns_()
                    ]
                ),
                primary_key=lineage.ColumnList(
                    [
                        lineage_columns.Select(column)
                        for column in source.primary_key.list_columns_()
                    ]
                ),
                event_time=lineage_columns.Select(source.event_time),
                source=source,
                schema=source.schema,
                macro_group=macro_group,
            )
        else:
            super().__init__(
                name=name,
                columns=lineage.ColumnList(
                    [
                        lineage_columns.Select(column)
                        for column in source.columns.list_columns_()
                    ]
                ),
                primary_key=lineage.ColumnList(
                    [
                        lineage_columns.Select(column)
                        for column in source.primary_key.list_columns_()
                    ]
                ),
                source=source,
                schema=source.schema,
                macro_group=macro_group,
            )

    def __deepcopy__(self, memodict={}):
        return Select(
            source=self.source,
            alias=self.name,
        )


class FromRecords(lineage._Table):
    def __init__(
        self,
        name: str,
        source: lineage.RecordList,
        primary_key: lineage.ColumnList = lineage.ColumnList([]),
        event_time: lineage._Column = None,
        macro_group: str = "",
    ):
        if event_time:
            super().__init__(
                name=name,
                source=source,
                columns=source.columns,
                primary_key=primary_key,
                event_time=event_time,
                macro_group=macro_group,
            )
        else:
            super().__init__(
                name=name,
                source=source,
                columns=source.columns,
                primary_key=primary_key,
                event_time=None,
                macro_group=macro_group,
            )

    def __deepcopy__(self, memodict={}):
        return FromRecords(
            name=self.name,
            source=self.source,
            primary_key=self.primary_key,
            event_time=self.event_time,
        )


class Insert:
    def __init__(self, source, target: Select, macro_group: str = ""):
        self.source = source
        self.target = target

        self._node_data = {
            "label": rf"INSERT - {str(id(self))}",
            "macro_group": macro_group,
        }

    def __deepcopy__(self, memodict={}):
        return Insert(source=self.source, target=self.target)


class Subquery(lineage._Table):
    def __init__(self, source, name: str = None, macro_group: str = "") -> None:
        if type(source) not in [Core, Union]:
            raise ValueError("Subquery source must be Core")

        if not name:
            name = source.name
        else:
            name = name

        if source.event_time:
            event_time = lineage_columns.Select(source.event_time)
        else:
            event_time = None

        super().__init__(
            name=name,
            columns=lineage.ColumnList(
                [
                    lineage_columns.Core(
                        source=lineage_columns.Select(column), name=column.name
                    )
                    for column in source.columns.list_columns_()
                ]
            ),
            source=source,
            primary_key=lineage.ColumnList(
                [
                    lineage_columns.Select(column)
                    for column in source.primary_key.list_columns_()
                ]
            ),
            event_time=event_time,
            macro_group=macro_group,
        )


class Union(lineage._Table):
    def __init__(
        self,
        name: str,
        tables: lineage.TableList,
        columns: lineage.ColumnList = lineage.ColumnList([]),
        primary_key: lineage.ColumnList = lineage.ColumnList([]),
        event_time: lineage._Column = None,
        macro_group: str = "",
    ) -> None:
        if not all(
            [type(column) is lineage_columns.Core for column in columns.list_columns_()]
        ):
            raise ValueError("Columns must be lineage_columns.Expand")

        if not all(
            [
                type(column) is lineage_columns.Core
                for column in primary_key.list_columns_()
            ]
        ):
            raise ValueError("Columns must be lineage_columns.Expand")

        if columns.is_empty:
            columns = lineage.ColumnList(
                [
                    lineage_columns.Core(
                        name=column.name,
                        source=lineage_functions.union.UnionColumn(
                            [
                                lineage_columns.Select(table.columns[column.name])
                                for table in tables.list_tables_()
                            ]
                        ),
                    )
                    for column in tables.list_tables_()[0].columns.list_columns_()
                    if all(
                        [
                            column.name in table.columns.list_names_()
                            for table in tables.list_tables_()
                        ]
                    )
                ]
            )

        super().__init__(
            name=name,
            columns=columns,
            source=tables,
            primary_key=primary_key,
            event_time=event_time,
            macro_group=macro_group,
        )


class Temp(lineage._Table):
    def __init__(self, table: Core, macro_group: str = ""):
        super().__init__(
            name=table.name,
            columns=table.columns,
            source=table,
            primary_key=table.primary_key,
            event_time=table.event_time,
            where_condition=table.where_condition,
            group_by=table.group_by,
            having_condition=table.having_condition,
            macro_group=macro_group,
        )
