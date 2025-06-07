import duckdb
import pandas as pd


# Athena is currently fucked. AWS wrangler seems like a really shit package.


class Response:
    def __init__(self, response, connection_type: str):
        self.response = response
        self.connection_type = connection_type

    def df(self):
        if type(self.response) is pd.DataFrame:
            return self.response
        elif self.connection_type == "duckdb":
            return self.response.df()


class Connection:
    def __init__(self, name: str, syntax: str, database: str = None):
        self.name = name
        self.database = database
        self.syntax = syntax

        if database:
            self.connection = duckdb.connect(database)
        else:
            self.connection = None

    def execute(self, query):
        if type(query) is str:
            if self.connection:
                try:
                    return Response(
                        self.connection.cursor().execute(query), self.syntax
                    )
                except:
                    print(
                        rf"""
                    ERROR RUNNING FOLLOWING QUERY:
                    {query}
                    """
                    )
                    self.connection.cursor().execute(query)

            else:
                raise AttributeError("No database connection defined")

        elif type(query) is pd.DataFrame:
            return Response(query, self.syntax)

    def available_functions(self):
        if self.connection:
            if self.syntax == "duckdb":
                return (
                    self.execute(
                        "SELECT DISTINCT function_name FROM duckdb_functions()"
                    )
                    .df()["function_name"]
                    .tolist()
                )
        else:
            raise AttributeError("No database connection defined")

    def close(self):
        if self.connection:
            if self.syntax == "duckdb":
                self.connection.close()
        else:
            raise AttributeError("No database connection defined")

    def open(self):
        if self.connection:
            if self.syntax == "duckdb":
                self.connection = duckdb.connect(self.database)
        else:
            raise AttributeError("No database connection defined")
