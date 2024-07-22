from ..lineage import core as lineage
from ..lineage import values as lineage_values
from ..lineage import columns as lineage_columns
from ..lineage import expressions as lineage_expressions
from typing import List as TypingList, Any
import re
import pandas as pd


class Limit(lineage._Transformation):
    def __init__(
        self, source, limit: lineage_values.Integer, offset: lineage_values.Integer
    ):
        super().__init__(name="LIMIT", source=source, args=[limit, offset])


class ReadCSV(lineage._Transformation):
    def __init__(
        self,
        source_file: lineage._SourceFile,
        union_by_name: lineage_values.Boolean = lineage_values.Boolean(False),
        headers: lineage_values.Boolean = lineage_values.Boolean(False),
        filename_column: lineage_columns.Blank = None,
    ):
        if filename_column:
            is_filename = lineage_values.Boolean(True)
        else:
            is_filename = lineage_values.Boolean(False)

        super().__init__(
            name="READ_CSV",
            source=source_file,
            args={
                "union_by_name": union_by_name,
                "header": headers,
                "filename": is_filename,
            },
        )

        self.filename_column = filename_column
