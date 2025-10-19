import typing

from ...lineage import core as lineage


class UnionColumn(lineage._Function):
    def __init__(
        self,
        source: typing.List[lineage._Column],
        macro_group: str = "",
    ):
        if not len(set([column.name for column in source])) == 1:
            raise ValueError(
                rf"Columns must have the same name: [{', '.join([column.current_table.name + '.' + column.name for column in source])}]"
            )

        if not len(set([column.data_type.name for column in source])) == 1:
            raise ValueError(
                rf"Columns must have the same data_type: [{', '.join([column.current_table.name + '.' + column.name + ': ' + column.data_type.name for column in source])}]"
            )

        if not len(set([column.var_type for column in source])) == 1:
            raise ValueError(
                rf"Columns must have the same var_type: [{', '.join([column.current_table.name + '.' + column.name + ': ' + str(column.var_type) for column in source])}]"
            )

        if not len(set([column.unit.name for column in source])) == 1:
            raise ValueError(
                rf"Columns must have the same unit: [{', '.join([column.current_table.name + '.' + column.name + ': ' + column.unit.name for column in source])}]"
            )

        if not len(set([column.on_null for column in source])) == 1:
            raise ValueError(
                rf"Columns must have the same behaviour on NULL: [{', '.join([column.current_table.name + '.' + column.name + ': ' + column.on_null for column in source])}]"
            )

        super().__init__(
            name="UNION_COLUMN",
            args=source,
            var_type=source[0].var_type,
            data_type=source[0].data_type,
            macro_group=macro_group,
        )
