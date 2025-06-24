from .syntax import duckdb as duckdb_syntax
import sqlparse
from src.beehive.database import connections
from .syntax.network import item_to_graph

interpreters = {"duckdb": duckdb_syntax}


class Interpreter:
    def __init__(self, syntax="duckdb") -> None:
        self.syntax = syntax

    def to_sql(self, item):

        return sqlparse.format(
            interpreters[self.syntax].item_to_query(item),
            reindent=True,
        )

    def to_network(self, item):
        return item_to_graph(item)
