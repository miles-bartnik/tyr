from ..lineage import core as lineage


class _Equal(lineage._Operator):
    def __init__(self):
        super().__init__(name="=")


class _NotEqual(lineage._Operator):
    def __init__(self):
        super().__init__(name="!=")


class _GreaterThan(lineage._Operator):
    def __init__(self):
        super().__init__(name=">")


class _LessThan(lineage._Operator):
    def __init__(self):
        super().__init__(name="<")


class _GreaterThanOrEqual(lineage._Operator):
    def __init__(self):
        super().__init__(name=">=")


class _LessThanOrEqual(lineage._Operator):
    def __init__(self):
        super().__init__(name="<=")


class _Add(lineage._Operator):
    def __init__(self):
        super().__init__(name="+")


class _Subtract(lineage._Operator):
    def __init__(self):
        super().__init__(name="-")


class _Divide(lineage._Operator):
    def __init__(self):
        super().__init__(name="/")


class _Multiply(lineage._Operator):
    def __init__(self):
        super().__init__(name="*")


class _Exponent(lineage._Operator):
    def __init__(self):
        super().__init__(name="**")


class _Join(lineage._Operator):
    def __init__(self, how: str):
        super().__init__(name=rf"{how.upper().strip(' JOIN')} JOIN")


class _As(lineage._Operator):
    def __init__(self):
        super().__init__(name="AS")


class Descending(lineage._Operator):
    def __init__(self):
        super().__init__(name="DESC")


class Ascending(lineage._Operator):
    def __init__(self):
        super().__init__(name="ASC")


class WildCard(lineage._Operator):
    def __init__(self):
        super().__init__(name="*")


class And(lineage._Operator):
    def __init__(self):
        super().__init__(name="AND")


class Or(lineage._Operator):
    def __init__(self):
        super().__init__(name="OR")


class _In(lineage._Operator):
    def __init__(self):
        super().__init__(name="IN")


class _Is(lineage._Operator):
    def __init__(self):
        super().__init__(name="IS")


class _Not(lineage._Operator):
    def __init__(self):
        super().__init__(name="NOT")


class _Like(lineage._Operator):
    def __init__(self):
        super().__init__(name="LIKE")
