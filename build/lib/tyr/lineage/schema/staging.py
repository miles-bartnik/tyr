from typing import Dict, List, Any
from ..core import TableList
from .core import _Schema, _SchemaSettings
from .source import Source
from ..macros.tables import staging_table_transform
import datetime


class StagingSettings(_SchemaSettings):
    def __init__(
        self,
        name,
        substitutions: Dict[Any, Any] = {},
        extensions: List[Dict[str, str]] = [],
        connection: Dict = {},
        min_event_time: datetime.datetime = None,
        max_event_time: datetime.datetime = None,
    ):
        super().__init__(
            name=name,
            substitutions=substitutions,
            extensions=extensions,
            connection=connection,
            configuration={
                "min_event_time": min_event_time,
                "max_event_time": max_event_time,
            },
        )


class Staging(_Schema):
    def __init__(self, source: Source, settings: StagingSettings):
        self.source = source

        tables = TableList([])

        for table in self.source.tables.list_tables_():
            staging_table = staging_table_transform(table, settings)

            tables.add_(staging_table)

        super().__init__(settings=settings, tables=tables)

        self.graph.union([self.source.graph])
        self.graph.add_edge(
            self.graph.node_index_from_data(self.source._node_data),
            self.graph.node_index_from_data(self._node_data),
        )
