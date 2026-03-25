from .syntax.core import DuckDB

interpreters = {"duckdb": DuckDB}


class Interpreter:
    def __init__(self, syntax="duckdb") -> None:
        self.syntax = interpreters[syntax]()

    def to_sql(self, item):
        return self.syntax.item_to_sql(item)
