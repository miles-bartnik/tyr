from ...lineage import core as lineage
from ...lineage import values as lineage_values
from ...lineage import expressions as lineage_expressions
from typing import List as TypingList, Any
import re
import pandas as pd


class JSONExtract(lineage._Function):
    def __init__(self, source, key):
        super().__init__(
            args=[source, key],
            name="JSON_EXTRACT",
            data_type=lineage_values.Datatype(source.data_type.value.strip("[]")),
            var_type=source.var_type,
        )
