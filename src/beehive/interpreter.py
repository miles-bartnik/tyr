from .syntax import duckdb as duckdb_syntax
import sqlparse
from .database import connections
from .syntax.network import item_to_graph
from .syntax.dataframes import item_to_dataframe

interpreters = {"duckdb": duckdb_syntax}


class Interpreter:
    def __init__(self, connection: connections.Connection) -> None:
        self.connection = connection

    def to_sql(self, item, expand: bool = False):
        return sqlparse.format(
            interpreters[self.connection.syntax].item_to_query(item, expand),
            reindent=True,
        )

    def to_network(self, item):
        return item_to_graph(item)

    def to_dataframe(self, item):
        return item_to_dataframe(item, conn=self.connection, interpreter=self)
