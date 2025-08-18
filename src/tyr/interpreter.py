from .syntax.core import DuckDB
from .network import Spider

interpreters = {"duckdb": DuckDB}


class Interpreter:
    def __init__(self, syntax="duckdb") -> None:
        self.syntax = interpreters[syntax]()
        self.spider = Spider()

    def to_sql(self, item):
        return self.syntax.item_to_sql(item)

    def to_network(self, item):
        return self.spider.item_to_graph(item)
