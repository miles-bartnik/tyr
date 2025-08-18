from ..lineage import core as lineage
from ..lineage import operators


class As(lineage._Expression):
    def __init__(self, left, right, macro_group: str = ""):
        super().__init__(
            left=left, right=right, operator=operators._As(), macro_group=macro_group
        )


class Equal(lineage._Expression):
    def __init__(self, left, right, macro_group: str = ""):
        super().__init__(
            left=left, right=right, operator=operators._Equal(), macro_group=macro_group
        )


class In(lineage._Expression):
    def __init__(self, left, right, macro_group: str = ""):
        super().__init__(
            left=left, right=right, operator=operators._In(), macro_group=macro_group
        )


class Is(lineage._Expression):
    def __init__(self, left, right, macro_group: str = ""):
        super().__init__(
            left=left, right=right, operator=operators._Is(), macro_group=macro_group
        )


class Not(lineage._Expression):
    def __init__(self, left, right, macro_group: str = ""):
        super().__init__(
            left=left, right=right, operator=operators._Not(), macro_group=macro_group
        )


class Like(lineage._Expression):
    def __init__(self, left, right, macro_group: str = ""):
        super().__init__(
            left=left, right=right, operator=operators._Like(), macro_group=macro_group
        )


class GreaterThan(lineage._Expression):
    def __init__(self, left, right, macro_group: str = ""):
        super().__init__(
            left=left,
            right=right,
            operator=operators._GreaterThan(),
            macro_group=macro_group,
        )


class NotEqual(lineage._Expression):
    def __init__(self, left, right, macro_group: str = ""):
        super().__init__(
            left=left,
            right=right,
            operator=operators._NotEqual(),
            macro_group=macro_group,
        )


class LessThan(lineage._Expression):
    def __init__(self, left, right, macro_group: str = ""):
        super().__init__(
            left=left,
            right=right,
            operator=operators._LessThan(),
            macro_group=macro_group,
        )


class GreaterThanOrEqual(lineage._Expression):
    def __init__(self, left, right, macro_group: str = ""):
        super().__init__(
            left=left,
            right=right,
            operator=operators._GreaterThanOrEqual(),
            macro_group=macro_group,
        )


class LessThanOrEqual(lineage._Expression):
    def __init__(self, left, right, macro_group: str = ""):
        super().__init__(
            left=left,
            right=right,
            operator=operators._LessThanOrEqual(),
            macro_group=macro_group,
        )


class LeftJoin(lineage._Expression):
    def __init__(self, left, right, macro_group: str = ""):
        super().__init__(
            left=left,
            right=right,
            operator=operators._Join(how="LEFT"),
            macro_group=macro_group,
        )


class RightJoin(lineage._Expression):
    def __init__(self, left, right, macro_group: str = ""):
        super().__init__(
            left=left,
            right=right,
            operator=operators._Join(how="RIGHT"),
            macro_group=macro_group,
        )


class InnerJoin(lineage._Expression):
    def __init__(self, left, right, macro_group: str = ""):
        super().__init__(
            left=left,
            right=right,
            operator=operators._Join(how="INNER"),
            macro_group=macro_group,
        )


class FullOuterJoin(lineage._Expression):
    def __init__(self, left, right, macro_group: str = ""):
        super().__init__(
            left=left,
            right=right,
            operator=operators._Join(how="FULL OUTER"),
            macro_group=macro_group,
        )


class And(lineage._Expression):
    def __init__(self, left, right, macro_group: str = ""):
        super().__init__(
            left=left, right=right, operator=operators.And(), macro_group=macro_group
        )


class Or(lineage._Expression):
    def __init__(self, left, right, macro_group: str = ""):
        super().__init__(
            left=left, right=right, operator=operators.Or(), macro_group=macro_group
        )


class Between(lineage._Expression):
    def __init__(self, left, right: And, macro_group: str = ""):
        super().__init__(
            left=left,
            right=right,
            operator=operators._Between(),
            macro_group=macro_group,
        )
