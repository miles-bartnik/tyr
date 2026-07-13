from ...lineage import core as lineage
from ...lineage import values as lineage_values


class JSONExtract(lineage._Function):
    """
    Extract a value from JSON at the given path (SQL json_extract).

    DuckDB: https://duckdb.org/docs/stable/data/json/json_functions#json-extraction-functions
    """
    def __init__(self, source, key):
        super().__init__(
            args=[source, key],
            name="JSON_EXTRACT",
            data_type=lineage_values.Datatype(source.data_type.value.strip("[]")),
            var_type=source.var_type,
        )
