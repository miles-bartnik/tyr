from tyr import lineage


class Check:
    def __init__(
        self,
        name: str,
        source,
        columns_checked: lineage.core.ColumnList,
        tables_checked: lineage.core.TableList,
        result,
        scope: str,
        granularity: lineage.values.Interval = None,
        success: lineage.core._Table = None,
        failed: lineage.core._Table = None,
    ):
        self.name = name
        self.source = source
        self.columns_checked = columns_checked
        self.tables_checked = tables_checked
        self.result = result
        self.success = success
        self.failed = failed
        self.granularity = granularity

        if self.granularity:
            self.type = "historic"
        else:
            self.type = "static"

        if scope in ["entity", "general"]:
            self.scope = scope
        else:
            raise ValueError("type must be in ['entity', 'general']")

        if self.type == "static":
            if self.scope == "general":
                self.check = lineage.tables.Core(
                    name=rf"check_insert",
                    columns=lineage.core.ColumnList(
                        [
                            lineage.columns.Core(
                                name="check_name", source=lineage.values.Varchar(name)
                            ),
                            lineage.columns.Core(
                                name="tables_checked",
                                source=lineage.values.List(
                                    [
                                        lineage.values.Varchar(item.name)
                                        for item in tables_checked.list_all()
                                    ]
                                ),
                            ),
                            lineage.columns.Core(
                                name="columns_checked",
                                source=lineage.values.List(
                                    [
                                        lineage.values.Varchar(item.name)
                                        for item in columns_checked.list_all()
                                    ]
                                ),
                            ),
                            lineage.columns.Core(
                                name="check_result",
                                source=result,
                            ),
                        ]
                    ),
                    source=source,
                    primary_key=lineage.core.ColumnList(
                        [
                            lineage.columns.Core(
                                name="check_name", source=lineage.values.Varchar(name)
                            ),
                            lineage.columns.Core(
                                name="tables_checked",
                                source=lineage.values.List(
                                    [
                                        lineage.values.Varchar(item.name)
                                        for item in tables_checked.list_all()
                                    ]
                                ),
                            ),
                            lineage.columns.Core(
                                name="columns_checked",
                                source=lineage.values.List(
                                    [
                                        lineage.values.Varchar(item.name)
                                        for item in columns_checked.list_all()
                                    ]
                                ),
                            ),
                        ]
                    ),
                    group_by=True,
                )
            elif self.scope == "entity":
                if len(self.tables_checked.list_all()) > 1:
                    raise ValueError("Entity checks can only be run on a single table")

                self.check = lineage.tables.Core(
                    name=rf"check_insert",
                    columns=lineage.core.ColumnList(
                        [
                            lineage.columns.Core(
                                name="check_name", source=lineage.values.Varchar(name)
                            ),
                            lineage.columns.Core(
                                name="tables_checked",
                                source=lineage.values.List(
                                    [
                                        lineage.values.Varchar(item.name)
                                        for item in tables_checked.list_all()
                                    ]
                                ),
                            ),
                            lineage.columns.Core(
                                name="entity_key",
                                source=lineage.values.List(
                                    [
                                        lineage.values.Varchar(column.name)
                                        for column in source.static_primary_key.list_all()
                                    ]
                                ),
                            ),
                            lineage.columns.Core(
                                name="entity_checked",
                                source=lineage.values.List(
                                    [
                                        lineage.functions.data_type.Cast(
                                            lineage.columns.Select(column),
                                            data_type=lineage.values.Datatype(
                                                "VARCHAR"
                                            ),
                                        )
                                        for column in source.static_primary_key.list_all()
                                    ]
                                ),
                            ),
                            lineage.columns.Core(
                                name="columns_checked",
                                source=lineage.values.List(
                                    [
                                        lineage.values.Varchar(item.name)
                                        for item in columns_checked.list_all()
                                    ]
                                ),
                            ),
                            lineage.columns.Core(
                                name="check_result",
                                source=result,
                            ),
                        ]
                    ),
                    primary_key=lineage.core.ColumnList(
                        [
                            lineage.columns.Core(
                                name="check_name", source=lineage.values.Varchar(name)
                            ),
                            lineage.columns.Core(
                                name="tables_checked",
                                source=lineage.values.List(
                                    [
                                        lineage.values.Varchar(item.name)
                                        for item in tables_checked.list_all()
                                    ]
                                ),
                            ),
                            lineage.columns.Core(
                                name="entity_checked",
                                source=lineage.values.List(
                                    [
                                        lineage.functions.data_type.Cast(
                                            lineage.columns.Select(column),
                                            data_type=lineage.values.Datatype(
                                                "VARCHAR"
                                            ),
                                        )
                                        for column in source.static_primary_key.list_all()
                                    ]
                                ),
                            ),
                        ]
                    ),
                    group_by=True,
                    source=source,
                )
        elif self.type == "historic":
            if self.scope == "general":
                self.check = lineage.tables.Core(
                    name=rf"check_insert",
                    columns=lineage.core.ColumnList(
                        [
                            lineage.columns.Core(
                                name="check_name", source=lineage.values.Varchar(name)
                            ),
                            lineage.columns.Core(
                                name="tables_checked",
                                source=lineage.values.List(
                                    [
                                        lineage.values.Varchar(item.name)
                                        for item in tables_checked.list_all()
                                    ]
                                ),
                            ),
                            lineage.columns.Core(
                                name="event_time_column",
                                source=lineage.values.Varchar(
                                    tables_checked.list_all()[0].event_time.name
                                ),
                            ),
                            lineage.columns.Core(
                                name="event_time",
                                source=lineage.functions.datetime.DateBin(
                                    source=lineage.columns.Select(
                                        tables_checked.list_all()[0].event_time
                                    ),
                                    interval=granularity,
                                ),
                            ),
                            lineage.columns.Core(
                                name="columns_checked",
                                source=lineage.values.List(
                                    [
                                        lineage.values.Varchar(item.name)
                                        for item in columns_checked.list_all()
                                    ]
                                ),
                            ),
                            lineage.columns.Core(
                                name="check_result",
                                source=result,
                            ),
                        ]
                    ),
                    source=source,
                    primary_key=lineage.core.ColumnList(
                        [
                            lineage.columns.Core(
                                name="check_name", source=lineage.values.Varchar(name)
                            ),
                            lineage.columns.Core(
                                name="tables_checked",
                                source=lineage.values.List(
                                    [
                                        lineage.values.Varchar(item.name)
                                        for item in tables_checked.list_all()
                                    ]
                                ),
                            ),
                            lineage.columns.Core(
                                name="columns_checked",
                                source=lineage.values.List(
                                    [
                                        lineage.values.Varchar(item.name)
                                        for item in columns_checked.list_all()
                                    ]
                                ),
                            ),
                            lineage.columns.Core(
                                name="event_time_column",
                                source=lineage.values.Varchar(
                                    tables_checked.list_all()[0].event_time.name
                                ),
                            ),
                            lineage.columns.Core(
                                name="event_time",
                                source=lineage.functions.datetime.DateBin(
                                    source=lineage.columns.Select(
                                        tables_checked.list_all()[0].event_time
                                    ),
                                    interval=granularity,
                                ),
                            ),
                        ]
                    ),
                    group_by=True,
                )
            elif self.scope == "entity":
                if len(self.tables_checked.list_all()) > 1:
                    raise ValueError("Entity checks can only be run on a single table")

                self.check = lineage.tables.Core(
                    name=rf"check_insert",
                    columns=lineage.core.ColumnList(
                        [
                            lineage.columns.Core(
                                name="check_name", source=lineage.values.Varchar(name)
                            ),
                            lineage.columns.Core(
                                name="tables_checked",
                                source=lineage.values.List(
                                    [
                                        lineage.values.Varchar(item.name)
                                        for item in tables_checked.list_all()
                                    ]
                                ),
                            ),
                            lineage.columns.Core(
                                name="event_time_column",
                                source=lineage.values.Varchar(
                                    tables_checked.list_all()[0].event_time.name
                                ),
                            ),
                            lineage.columns.Core(
                                name="event_time",
                                source=lineage.functions.datetime.DateBin(
                                    source=lineage.columns.Select(
                                        tables_checked.list_all()[0].event_time
                                    ),
                                    interval=granularity,
                                ),
                            ),
                            lineage.columns.Core(
                                name="entity_key",
                                source=lineage.values.List(
                                    [
                                        lineage.values.Varchar(column.name)
                                        for column in source.static_primary_key.list_all()
                                    ]
                                ),
                            ),
                            lineage.columns.Core(
                                name="entity_checked",
                                source=lineage.values.List(
                                    [
                                        lineage.functions.data_type.Cast(
                                            lineage.columns.Select(column),
                                            data_type=lineage.values.Datatype(
                                                "VARCHAR"
                                            ),
                                        )
                                        for column in source.static_primary_key.list_all()
                                    ]
                                ),
                            ),
                            lineage.columns.Core(
                                name="columns_checked",
                                source=lineage.values.List(
                                    [
                                        lineage.values.Varchar(item.name)
                                        for item in columns_checked.list_all()
                                    ]
                                ),
                            ),
                            lineage.columns.Core(
                                name="check_result",
                                source=result,
                            ),
                        ]
                    ),
                    primary_key=lineage.core.ColumnList(
                        [
                            lineage.columns.Core(
                                name="check_name", source=lineage.values.Varchar(name)
                            ),
                            lineage.columns.Core(
                                name="tables_checked",
                                source=lineage.values.List(
                                    [
                                        lineage.values.Varchar(item.name)
                                        for item in tables_checked.list_all()
                                    ]
                                ),
                            ),
                            lineage.columns.Core(
                                name="event_time_column",
                                source=lineage.values.Varchar(
                                    tables_checked.list_all()[0].event_time.name
                                ),
                            ),
                            lineage.columns.Core(
                                name="event_time",
                                source=lineage.functions.datetime.DateBin(
                                    source=lineage.columns.Select(
                                        tables_checked.list_all()[0].event_time
                                    ),
                                    interval=granularity,
                                ),
                            ),
                            lineage.columns.Core(
                                name="entity_checked",
                                source=lineage.values.List(
                                    [
                                        lineage.functions.data_type.Cast(
                                            lineage.columns.Select(column),
                                            data_type=lineage.values.Datatype(
                                                "VARCHAR"
                                            ),
                                        )
                                        for column in source.static_primary_key.list_all()
                                    ]
                                ),
                            ),
                        ]
                    ),
                    group_by=True,
                    source=source,
                )
