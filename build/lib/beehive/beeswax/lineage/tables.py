from ..lineage import core as lineage
from ..lineage import columns as lineage_columns
from ..lineage import values as lineage_values
from ..lineage import joins as lineage_combinations
from ..lineage import expressions as lineage_expressions
from ..lineage import operators as lineage_operators
from ..lineage import functions as lineage_functions
import networkx as nx
import pandas as pd


class SourceFile(lineage._SourceFile):
    def __init__(self, file_metadata: pd.DataFrame, columns: lineage.ColumnList):
        delim = file_metadata["delim"].iloc[0]

        if delim == "c":
            self.delim = ","
        else:
            self.delim = "\\" + delim

        super().__init__(
            dataset=lineage_values.Varchar(file_metadata["dataset"].iloc[0]),
            file_regex=lineage_values.Varchar(file_metadata["file_regex"].iloc[0]),
            delim=lineage_values.Varchar(delim),
            columns=columns,
        )


class Core(lineage._Table):
    def __init__(
        self,
        name,
        columns: lineage.ColumnList = lineage.ColumnList([]),
        source=None,
        primary_key: lineage.ColumnList = lineage.ColumnList([]),
        event_time=None,
        distinct: bool = False,
        group_by: bool = False,
        where_condition: lineage.Condition = None,
        having_condition: lineage.Condition = None,
        ctes=None,
    ) -> None:
        if columns.is_empty:
            columns = lineage_columns.select_all(source.columns)

        if not all(
            [
                type(column) in [lineage_columns.Core, lineage_columns.Blank]
                for column in columns.list_all()
            ]
        ):
            raise ValueError("All columns must be Core or Blank")

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
        )

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
    def __init__(self, source, alias: str = None) -> None:
        if type(source).__bases__[0] is lineage._Table:
            if alias:
                name = alias
            else:
                name = source.name

        else:
            raise ValueError(rf"Invalid source - {type(source)}")

        if source.event_time:
            super().__init__(
                name=name,
                columns=lineage_columns.select_all(source.columns),
                primary_key=lineage_columns.select_all(source.primary_key),
                event_time=lineage_columns.Select(source.event_time),
                source=source,
                schema=source.schema,
            )
        else:
            super().__init__(
                name=name,
                columns=lineage_columns.select_all(source.columns),
                primary_key=lineage_columns.select_all(source.primary_key),
                source=source,
                schema=source.schema,
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
    ):
        super().__init__(
            name=name,
            source=source,
            columns=source.columns,
            primary_key=primary_key,
            event_time=None,
        )

    def __deepcopy__(self, memodict={}):
        return FromRecords(
            name=self.name,
            source=self.source,
            primary_key=self.primary_key,
            event_time=self.event_time,
        )


class Insert:
    def __init__(self, source, target: Select):
        self.source = source
        self.target = target

    def __deepcopy__(self, memodict={}):
        return Insert(source=self.source, target=self.target)


class Subquery(lineage._Table):
    def __init__(self, source, name: str = None) -> None:
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
        )


class Union(lineage._Table):
    def __init__(
        self,
        name: str,
        tables: lineage.TableList,
        columns: lineage.ColumnList = lineage.ColumnList([]),
        primary_key: lineage.ColumnList = lineage.ColumnList([]),
        event_time: lineage._Column = None,
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
                        source=lineage_functions.union.UnionColumn(
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
        )


class Temp(lineage._Table):
    def __init__(self, table: Core):
        super().__init__(
            name=table.name,
            columns=table.columns,
            source=table,
            primary_key=table.primary_key,
            event_time=table.event_time,
            where_condition=table.where_condition,
            group_by=table.group_by,
            having_condition=table.having_condition,
        )
