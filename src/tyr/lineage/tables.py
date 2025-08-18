from ..lineage import core as lineage
from ..lineage import columns as lineage_columns


class Core(lineage._Table):
    def __init__(
        self,
        name,
        columns,
        source=None,
        primary_key: lineage.ColumnList = lineage.ColumnList([]),
        event_time=None,
        distinct: bool = False,
        group_by: bool = False,
        where_condition: lineage.Condition = None,
        having_condition: lineage.Condition = None,
        ctes=None,
        macro_group: str = "",
    ) -> None:
        if not all(
            [isinstance(column, lineage._Column) for column in columns.list_all()]
        ):
            print([rf"{column.name} - {type(column)}" for column in columns.list_all()])
            raise ValueError("All columns must be Core, Blank, or WildCard")

        if ctes:
            for table in ctes.list_all():
                if type(table) not in [Core, Select]:
                    print(table.__dict__)
                    raise ValueError("cte must be either Core or Select")

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
                        for column in source.columns.list_all()
                    ]
                ),
                primary_key=lineage.ColumnList(
                    [
                        lineage_columns.Select(column)
                        for column in source.primary_key.list_all()
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
                        for column in source.columns.list_all()
                    ]
                ),
                primary_key=lineage.ColumnList(
                    [
                        lineage_columns.Select(column)
                        for column in source.primary_key.list_all()
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
                    for column in source.columns.list_all()
                ]
            ),
            source=source,
            primary_key=lineage.ColumnList(
                [
                    lineage_columns.Select(column)
                    for column in source.primary_key.list_all()
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
            [type(column) is lineage_columns.Core for column in columns.list_all()]
        ):
            raise ValueError("Columns must be lineage_columns.Expand")

        if not all(
            [type(column) is lineage_columns.Core for column in primary_key.list_all()]
        ):
            raise ValueError("Columns must be lineage_columns.Expand")

        if columns.is_empty:
            columns = lineage.ColumnList(
                [
                    lineage_columns.Core(
                        name=column.name,
                        source=tyr.lineage.functions.union.UnionColumn(
                            [
                                lineage_columns.Select(table.columns[column.name])
                                for table in tables.list_all()
                            ]
                        ),
                    )
                    for column in tables.list_all()[0].columns.list_all()
                    if all(
                        [
                            column.name in table.columns.list_names()
                            for table in tables.list_all()
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
