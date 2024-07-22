from ..schema import Schema, SchemaSettings
from typing import List, Dict
import pandas as pd
import re
from ...lineage import core as lineage
from ...lineage import tables as lineage_tables
from ...lineage import columns as lineage_columns
from ..macros import functions as macro_functions
from ..macros import columns as macro_columns
from ...lineage import transformations as lineage_transformations
from ...lineage import values as lineage_values


class SourceSettings(SchemaSettings):
    def __init__(
        self,
        name: str,
        file_metadata: pd.DataFrame,
        column_metadata: pd.DataFrame,
        substitutions: Dict = {},
        extensions: List[str] = [],
        connection: Dict = {"enable_progress_bar": True, "threads": 4},
    ) -> None:
        super().__init__(
            name=name,
            substitutions=substitutions,
            extensions=extensions,
            connection=connection,
        )

        self.file_metadata = file_metadata.fillna("")
        self.column_metadata = column_metadata.fillna("")


class StagingSettings(SchemaSettings):
    def __init__(
        self,
        name: str,
        substitutions: Dict = {},
        extensions: List[str] = [],
        connection: Dict = {"enable_progress_bar": True, "threads": 4},
        filename_column: lineage_columns.Select = None,
    ) -> None:
        super().__init__(
            name=name,
            substitutions=substitutions,
            extensions=extensions,
            connection=connection,
        )

        self.filename_column = filename_column


class Source(Schema):
    def __init__(self, settings: SourceSettings) -> None:
        tables = []

        file_metadata = settings.file_metadata.fillna("")
        column_metadata = settings.column_metadata.fillna("")

        column_metadata["column_alias"] = column_metadata.apply(
            lambda row: (
                re.sub(
                    "_+",
                    "_",
                    re.sub("\.", "_", re.sub("[A-Z]", "_\1", row["column_name"])),
                ).lower()
                if not row["column_alias"]
                else row["column_alias"]
            ),
            axis=1,
        )

        for dataset in file_metadata.dataset.unique():
            source_file_columns = lineage.ColumnList([])

            for column_name in (
                column_metadata[column_metadata["dataset"] == dataset]
                .sort_values(by="ordinal_position", ascending=True)["column_name"]
                .unique()
            ):
                for column_alias in column_metadata[
                    (column_metadata["dataset"] == dataset)
                    & (column_metadata["column_name"] == column_name)
                ]["column_alias"].unique():
                    source_file_columns.add(
                        lineage_columns.Source(
                            column_metadata=column_metadata[
                                (column_metadata["dataset"] == dataset)
                                & (column_metadata["column_name"] == column_name)
                                & (column_metadata["column_alias"] == column_alias)
                            ]
                        )
                    )

            source_file = lineage_tables.SourceFile(
                file_metadata=file_metadata[file_metadata["dataset"] == dataset],
                columns=source_file_columns,
            )

            source_table = lineage_tables.Core(
                name=rf"""{dataset}""",
                columns=lineage.ColumnList(
                    [
                        macro_columns.source_transform(column)
                        for column in source_file.columns.list_all()
                    ]
                    + [
                        lineage_columns.Blank(
                            "filename",
                            var_type="str",
                            data_type=lineage_values.Datatype("VARCHAR"),
                        )
                    ]
                ),
                distinct=True,
                source=lineage_transformations.ReadCSV(
                    source_file=source_file,
                    union_by_name=lineage_values.Boolean(True),
                    headers=lineage_values.Boolean(True),
                    filename_column=lineage_columns.Blank(
                        "filename",
                        var_type="str",
                        data_type=lineage_values.Datatype("VARCHAR"),
                    ),
                ),
            )

            if (
                len(
                    [
                        lineage_columns.Select(column)
                        for column in source_file.columns.list_all()
                        if column.is_event_time
                    ]
                )
                == 1
            ):
                source_table.set_event_time(
                    [
                        lineage_columns.Select(column)
                        for column in source_file.columns.list_all()
                        if column.is_event_time
                    ][0]
                )

            source_table.set_primary_key(
                lineage.ColumnList(
                    [
                        lineage_columns.Select(column)
                        for column in source_file.columns.list_all()
                        if column.is_primary_key
                    ]
                )
            )

            tables.append(source_table)

        super().__init__(settings=settings, tables=lineage.TableList(tables))


class Staging(Schema):
    def __init__(self, settings: SchemaSettings, source: Source) -> None:
        tables = []
        exclude_names = []

        if settings.filename_column:
            exclude_names.append([settings.filename_column])

        for table in source.tables.list_all():
            source_table = lineage_tables.Select(
                source=table,
            )

            if source_table.event_time:
                staging_table = lineage_tables.Core(
                    name=table.name,
                    source=source_table,
                    columns=lineage.ColumnList(
                        [
                            lineage_columns.Core(
                                lineage_columns.Select(column), name=column.name
                            )
                            for column in source_table.columns.list_all()
                            if column.name not in exclude_names
                        ]
                    ),
                    primary_key=lineage.ColumnList(
                        [
                            lineage_columns.Select(column)
                            for column in source_table.primary_key.list_all()
                            if column.name not in exclude_names
                        ]
                    ),
                    event_time=lineage_columns.Select(source_table.event_time),
                )

            else:
                staging_table = lineage_tables.Core(
                    name=table.name,
                    source=source_table,
                    columns=lineage.ColumnList(
                        [
                            lineage_columns.Core(
                                lineage_columns.Select(column), name=column.name
                            )
                            for column in source_table.columns.list_all()
                            if column.name not in exclude_names
                        ]
                    ),
                    primary_key=lineage.ColumnList(
                        [
                            lineage_columns.Select(column)
                            for column in source_table.primary_key.list_all()
                            if column.name not in exclude_names
                        ]
                    ),
                )

            tables.append(staging_table)

        super().__init__(settings=settings, tables=lineage.TableList(tables))


class Merging(Schema):
    def __init__(self, settings: SchemaSettings, staging: Staging) -> None:
        super().__init__(settings=settings, tables=lineage.TableList([]))

        for table in staging.tables.list_all():
            staging_table = lineage_tables.Select(
                source=table,
            )

            if staging_table.event_time:
                merging_table = lineage_tables.Core(
                    name=table.name,
                    source=staging_table,
                    columns=lineage.ColumnList(
                        [
                            lineage_columns.Core(
                                lineage_columns.Select(column), name=column.name
                            )
                            for column in staging_table.columns.list_all()
                        ]
                    ),
                    primary_key=lineage.ColumnList(
                        [
                            lineage_columns.Select(column)
                            for column in staging_table.primary_key.list_all()
                        ]
                    ),
                    event_time=lineage_columns.Select(staging_table.event_time),
                )

            else:
                merging_table = lineage_tables.Core(
                    name=table.name,
                    source=staging_table,
                    columns=lineage.ColumnList(
                        [
                            lineage_columns.Core(
                                lineage_columns.Select(column), name=column.name
                            )
                            for column in staging_table.columns.list_all()
                        ]
                    ),
                    primary_key=lineage.ColumnList(
                        [
                            lineage_columns.Select(column)
                            for column in staging_table.primary_key.list_all()
                        ]
                    ),
                )

            self.tables.add(merging_table)
