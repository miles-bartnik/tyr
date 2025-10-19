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


# class ConnectionSettings:
#
#     def __init__(self, syntax="duckdb"):


class Connection:
    def __init__(
        self, name: str, syntax: str, database: str = None, read_only: bool = False
    ):
        self.name = name
        self.database = database
        self.syntax = syntax
        self.read_only = read_only

        if database:
            self.connection = duckdb.connect(database, read_only=self.read_only)
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

    def tables(self, schema: str = None):
        if self.connection:
            if self.syntax == "duckdb":
                filter_str = ""

                if schema:
                    filter_str += rf" WHERE table_schema = '{schema}'"
                return self.execute(
                    rf"SELECT * FROM information_schema.tables{filter_str}"
                ).df()

    def columns(self, schema: str = None, table: str = None):
        if self.connection:
            if self.syntax == "duckdb":
                components = [
                    rf"table_schema = '{schema}'" if schema else "",
                    rf"table_name = '{table}'" if table else "",
                ]

                if any([schema, table]):
                    filter_str = rf" WHERE {' AND '.join([component for component in components if component!=''])}"
                else:
                    filter_str = ""

                return self.execute(
                    rf"SELECT * FROM information_schema.columns{filter_str}"
                ).df()

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
