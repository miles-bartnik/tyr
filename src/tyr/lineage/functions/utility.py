from ...lineage import core as lineage
from ...lineage import values as lineage_values
from ...lineage import columns as lineage_columns
from typing import List as TypingList, Any
import pandas as pd


class Error(lineage._Function):
    def __init__(self, message: lineage_values.Varchar, macro_group: str = ""):
        super().__init__(args=[message], name="ERROR", macro_group=macro_group)


class Coalesce(lineage._Function):
    def __init__(self, args: TypingList[Any], macro_group: str = ""):
        super().__init__(args=args, name="COALESCE", macro_group=macro_group)


class SourceWildToStagingColumn(lineage._Function):

    """
    This is a solution to link the source wildcard select to the corresponding staging column.
    It is a weird solution. I'd like a better one.
    """

    def __init__(
        self,
        source: lineage_columns.WildCard(),
        column_metadata,
        macro_group: str = "",
    ):
        if "lineage.schema.source.ColumnMetadata" not in str(type(column_metadata)):
            raise ValueError(
                "column_metadata must be lineage.schema.source.ColumnMetadata object"
            )

        super().__init__(
            args=[source, column_metadata],
            name="SOURCE_WILD_TO_STAGING_COLUMN",
            macro_group=macro_group,
        )
