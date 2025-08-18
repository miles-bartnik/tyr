from ...lineage import core as lineage
from ...lineage import values as lineage_values


class JSONExtract(lineage._Function):
    def __init__(self, source, key, macro_group: str = ""):
        super().__init__(
            args=[source, key],
            name="JSON_EXTRACT",
            data_type=lineage_values.Datatype(source.data_type.value.strip("[]")),
            var_type=source.var_type,
            macro_group=macro_group,
        )
