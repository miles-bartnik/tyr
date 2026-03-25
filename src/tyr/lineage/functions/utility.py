from ...lineage import core as lineage
from ...lineage import values as lineage_values
from typing import List as TypingList, Any
import pandas as pd


class Error(lineage._Function):
    def __init__(self, message: lineage_values.Varchar):
        super().__init__(args=[message], name="ERROR")


class Coalesce(lineage._Function):
    def __init__(self, args: TypingList[Any]):
        if not all([arg.unit == args[0].unit for arg in args]):
            raise ValueError(
                rf"All arguments must have the same unit as the source argument: '{args[0].unit.name}'"
            )

        super().__init__(args=args, name="COALESCE", unit=args[0].unit)


class SourceWildToStagingColumn(lineage._Function):

    """
    This is a solution to link the source wildcard select to the corresponding staging column.
    It is a weird solution. I'd like a better one.
    """

    def __init__(
        self,
        source,
        column_metadata,
    ):
        if "lineage.schema.source.ColumnMetadata" not in str(type(column_metadata)):
            raise ValueError(
                "column_metadata must be lineage.schema.source.ColumnMetadata object"
            )

        if "lineage.columns.WildCard" not in str(type(source)):
            raise ValueError(
                rf"source must be lineage.columns.WildCard object. Encountered: {str(type(source))}"
            )

        super().__init__(
            args=[source, column_metadata],
            name="SOURCE_WILD_TO_STAGING_COLUMN",
            unit=column_metadata.source_unit,
        )
