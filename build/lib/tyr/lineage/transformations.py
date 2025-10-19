from ..lineage import core as lineage
from ..lineage import values as lineage_values


class Limit(lineage._Transformation):
    def __init__(
        self,
        source,
        limit: lineage_values.Integer,
        offset: lineage_values.Integer = lineage_values.Integer(0),
        macro_group: str = "",
    ):
        super().__init__(
            name="LIMIT", source=source, args=[limit, offset], macro_group=macro_group
        )
