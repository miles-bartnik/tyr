from ..lineage import core as lineage
from ..lineage import values as lineage_values
from .schema import source


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


class ReadCSV(lineage._Transformation):
    def __init__(
        self,
        source_file: source.SourceFile,
        union_by_name: lineage_values.Boolean = lineage_values.Boolean(False),
        headers: lineage_values.Boolean = lineage_values.Boolean(False),
        macro_group: str = "",
    ):
        super().__init__(
            name="READ_CSV",
            source=source_file,
            args={
                "union_by_name": union_by_name,
                "header": headers,
            },
            macro_group=macro_group,
        )


class ReadGeoJson(lineage._Transformation):
    def __init__(
        self,
        source_file: source.SourceFile,
        macro_group: str = "",
    ):
        super().__init__(
            name="ST_READ", source=source_file, args={}, macro_group=macro_group
        )
