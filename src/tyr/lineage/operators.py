from ..lineage import core as lineage


class _Equal(lineage._Operator):
    def __init__(self, macro_group: str = ""):
        super().__init__(name="=", macro_group=macro_group)


class _NotEqual(lineage._Operator):
    def __init__(self, macro_group: str = ""):
        super().__init__(name="!=", macro_group=macro_group)


class _GreaterThan(lineage._Operator):
    def __init__(self, macro_group: str = ""):
        super().__init__(name=">", macro_group=macro_group)


class _LessThan(lineage._Operator):
    def __init__(self, macro_group: str = ""):
        super().__init__(name="<", macro_group=macro_group)


class _GreaterThanOrEqual(lineage._Operator):
    def __init__(self, macro_group: str = ""):
        super().__init__(name=">=", macro_group=macro_group)


class _LessThanOrEqual(lineage._Operator):
    def __init__(self, macro_group: str = ""):
        super().__init__(name="<=", macro_group=macro_group)


class _Add(lineage._Operator):
    def __init__(self, macro_group: str = ""):
        super().__init__(name="+", macro_group=macro_group)


class _Subtract(lineage._Operator):
    def __init__(self, macro_group: str = ""):
        super().__init__(name="-", macro_group=macro_group)


class _Divide(lineage._Operator):
    def __init__(self, macro_group: str = ""):
        super().__init__(name="/", macro_group=macro_group)


class _Multiply(lineage._Operator):
    def __init__(self, macro_group: str = ""):
        super().__init__(name="*", macro_group=macro_group)


class _Exponent(lineage._Operator):
    def __init__(self, macro_group: str = ""):
        super().__init__(name="**", macro_group=macro_group)


class _Join(lineage._Operator):
    def __init__(self, how: str, macro_group: str = ""):
        super().__init__(
            name=rf"{how.upper().strip(' JOIN')} JOIN", macro_group=macro_group
        )


class _As(lineage._Operator):
    def __init__(self, macro_group: str = ""):
        super().__init__(name="AS", macro_group=macro_group)


class Descending(lineage._Operator):
    def __init__(self, macro_group: str = ""):
        super().__init__(name="DESC", macro_group=macro_group)


class Ascending(lineage._Operator):
    def __init__(self, macro_group: str = ""):
        super().__init__(name="ASC", macro_group=macro_group)


class WildCard(lineage._Operator):
    def __init__(self, macro_group: str = ""):
        super().__init__(name="*", macro_group=macro_group)


class And(lineage._Operator):
    def __init__(self, macro_group: str = ""):
        super().__init__(name="AND", macro_group=macro_group)


class Or(lineage._Operator):
    def __init__(self, macro_group: str = ""):
        super().__init__(name="OR", macro_group=macro_group)


class _In(lineage._Operator):
    def __init__(self, macro_group: str = ""):
        super().__init__(name="IN", macro_group=macro_group)


class _Is(lineage._Operator):
    def __init__(self, macro_group: str = ""):
        super().__init__(name="IS", macro_group=macro_group)


class _Not(lineage._Operator):
    def __init__(self, macro_group: str = ""):
        super().__init__(name="NOT", macro_group=macro_group)


class _Like(lineage._Operator):
    def __init__(self, macro_group: str = ""):
        super().__init__(name="LIKE", macro_group=macro_group)


class _Between(lineage._Operator):
    def __init__(self, macro_group: str = ""):
        super().__init__(name="BETWEEN", macro_group=macro_group)
