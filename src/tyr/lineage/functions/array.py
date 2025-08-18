from ...lineage import core as lineage
from ...lineage import values as lineage_values


class Length(lineage._Function):
    def __init__(self, source, macro_group: str = ""):
        super().__init__(
            args=[source],
            name="LENGTH",
            data_type=lineage_values.Datatype("INTEGER"),
            var_type="numeric",
            macro_group=macro_group,
        )


class QuantileCont(lineage._Function):
    def __init__(
        self,
        source,
        macro_group: str = "",
    ):
        super().__init__(
            args=[source],
            name="LENGTH",
            data_type=lineage_values.Datatype("INTEGER"),
            var_type="numeric",
            macro_group=macro_group,
        )


class Unnest(lineage._Function):
    def __init__(
        self,
        source,
        macro_group: str = "",
    ):
        self.source = source

        if source.data_type:
            data_type = lineage_values.Datatype(source.data_type.value.strip("[]"))
        else:
            data_type = None

        super().__init__(
            name="UNNEST",
            args=[source],
            var_type=source.var_type,
            data_type=data_type,
            macro_group=macro_group,
        )


class Range(lineage._Function):
    def __init__(self, start, end, interval, macro_group: str = ""):
        self.start = start
        self.end = end
        self.interval = interval

        super().__init__(
            name="RANGE",
            args=[start, end, interval],
            var_type=start.var_type,
            data_type=start.data_type,
            macro_group=macro_group,
        )


class List(lineage._Function):
    def __init__(self, values, macro_group: str = ""):
        if len(list(set([value.data_type.name for value in values]))) > 1:
            raise ValueError("Mixed data_types provided in values")

        super().__init__(
            args=values,
            name="LIST",
            data_type=values[0].data_type,
            var_type=values[0].var_type,
            macro_group=macro_group,
        )


class ListExtract(lineage._Function):
    def __init__(self, source, elements, macro_group: str = ""):
        if type(elements) is not lineage_values.List:
            raise ValueError("elements must be lineage_values.List")

        if any([type(value) is not lineage_values.Integer for value in elements.value]):
            raise ValueError("All elements must be lineage_values.Integer")

        super().__init__(
            args=[source, elements],
            name="LIST_EXTRACT",
            data_type=lineage_values.Datatype(source.data_type.value.strip("[]")),
            var_type=source.var_type,
            macro_group=macro_group,
        )


class Maximum(lineage._Function):
    def __init__(
        self,
        source,
        macro_group: str = "",
    ):
        super().__init__(
            name="LIST_MAX",
            args=[source],
            data_type=lineage_values.Datatype(source.data_type.value.strip("[]")),
            var_type=source.var_type,
            macro_group=macro_group,
        )


class Minimum(lineage._Function):
    def __init__(
        self,
        source,
        macro_group: str = "",
    ):
        super().__init__(
            name="LIST_MIN",
            args=[source],
            data_type=lineage_values.Datatype(source.data_type.value.strip("[]")),
            var_type=source.var_type,
            macro_group=macro_group,
        )


class Contains(lineage._Function):
    def __init__(self, source, element, macro_group: str = ""):
        super().__init__(
            name="LIST_CONTAINS",
            args=[source, element],
            data_type=lineage_values.Datatype("BOOLEAN"),
            var_type=source.var_type,
            macro_group=macro_group,
        )
