from ..lineage import core as lineage
from ..lineage import operators


class As(lineage._Expression):
    def __init__(self, left, right):
        super().__init__(left=left, right=right, operator=operators._As())


class Equal(lineage._Expression):
    def __init__(self, left, right):
        super().__init__(left=left, right=right, operator=operators._Equal())


class In(lineage._Expression):
    def __init__(self, left, right):
        super().__init__(left=left, right=right, operator=operators._In())


class Is(lineage._Expression):
    def __init__(self, left, right):
        super().__init__(left=left, right=right, operator=operators._Is())


class Not(lineage._Expression):
    def __init__(self, left, right):
        super().__init__(left=left, right=right, operator=operators._Not())


class Like(lineage._Expression):
    def __init__(self, left, right):
        super().__init__(left=left, right=right, operator=operators._Like())


class GreaterThan(lineage._Expression):
    def __init__(self, left, right):
        super().__init__(left=left, right=right, operator=operators._GreaterThan())


class NotEqual(lineage._Expression):
    def __init__(self, left, right):
        super().__init__(left=left, right=right, operator=operators._NotEqual())


class LessThan(lineage._Expression):
    def __init__(self, left, right):
        super().__init__(left=left, right=right, operator=operators._LessThan())


class GreaterThanOrEqual(lineage._Expression):
    def __init__(self, left, right):
        super().__init__(
            left=left, right=right, operator=operators._GreaterThanOrEqual()
        )


class LessThanOrEqual(lineage._Expression):
    def __init__(self, left, right):
        super().__init__(left=left, right=right, operator=operators._LessThanOrEqual())


class LeftJoin(lineage._Expression):
    def __init__(self, left, right):
        super().__init__(left=left, right=right, operator=operators._Join(how="LEFT"))


class RightJoin(lineage._Expression):
    def __init__(self, left, right):
        super().__init__(left=left, right=right, operator=operators._Join(how="RIGHT"))


class InnerJoin(lineage._Expression):
    def __init__(self, left, right):
        super().__init__(left=left, right=right, operator=operators._Join(how="INNER"))


class FullOuterJoin(lineage._Expression):
    def __init__(self, left, right):
        super().__init__(
            left=left, right=right, operator=operators._Join(how="FULL OUTER")
        )


class And(lineage._Expression):
    def __init__(self, left, right):
        super().__init__(left=left, right=right, operator=operators.And())


class Or(lineage._Expression):
    def __init__(self, left, right):
        super().__init__(left=left, right=right, operator=operators.Or())


class Between(lineage._Expression):
    def __init__(self, left, right: And):
        super().__init__(left=left, right=right, operator=operators._Between())
