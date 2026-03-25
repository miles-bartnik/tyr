from ..lineage import core as lineage
from ..lineage import operators


class As(lineage._Expression):

    """
    :param left:
    :type right: _Column|_Value
    :param right:
    :type right: _Column|_Value
    """

    def __init__(self, left, right):
        super().__init__(left=left, right=right, operator=operators._As())


class In(lineage._Expression):

    """
    :param left:
    :type right: _Column|_Value
    :param right:
    :type right: _Column|_Value
    """

    def __init__(self, left, right):
        super().__init__(left=left, right=right, operator=operators._In())


class Is(lineage._Expression):

    """
    :param left:
    :type right: _Column|_Value
    :param right:
    :type right: _Column|_Value
    """

    def __init__(self, left, right):
        super().__init__(left=left, right=right, operator=operators._Is())


class Not(lineage._Expression):

    """
    :param left:
    :type right: _Column|_Value
    :param right:
    :type right: _Column|_Value
    """

    def __init__(self, left, right):
        super().__init__(left=left, right=right, operator=operators.Not())


class Like(lineage._Expression):

    """
    :param left:
    :type right: _Column|_Value
    :param right:
    :type right: _Column|_Value
    """

    def __init__(self, left, right):
        super().__init__(left=left, right=right, operator=operators._Like())


class Equal(lineage._Expression):

    """
    :param left:
    :type right: _Column|_Value
    :param right:
    :type right: _Column|_Value
    """

    def __init__(self, left, right):
        super().__init__(left=left, right=right, operator=operators._Equal())


class NotEqual(lineage._Expression):

    """
    :param left:
    :type right: _Column|_Value
    :param right:
    :type right: _Column|_Value
    """

    def __init__(self, left, right):
        super().__init__(
            left=left,
            right=right,
            operator=operators._NotEqual(),
        )


class GreaterThan(lineage._Expression):

    """
    :param left:
    :type right: _Column|_Value
    :param right:
    :type right: _Column|_Value
    """

    def __init__(self, left, right):
        super().__init__(
            left=left,
            right=right,
            operator=operators._GreaterThan(),
        )


class LessThan(lineage._Expression):

    """
    :param left:
    :type right: _Column|_Value
    :param right:
    :type right: _Column|_Value
    """

    def __init__(self, left, right):
        super().__init__(
            left=left,
            right=right,
            operator=operators._LessThan(),
        )


class GreaterThanOrEqual(lineage._Expression):

    """
    :param left:
    :type right: _Column|_Value
    :param right:
    :type right: _Column|_Value
    """

    def __init__(self, left, right):
        super().__init__(
            left=left,
            right=right,
            operator=operators._GreaterThanOrEqual(),
        )


class LessThanOrEqual(lineage._Expression):

    """
    :param left:
    :type right: _Column|_Value
    :param right:
    :type right: _Column|_Value
    """

    def __init__(self, left, right):
        super().__init__(
            left=left,
            right=right,
            operator=operators._LessThanOrEqual(),
        )


class LeftJoin(lineage._Expression):

    """
    :param left:
    :type right: _Column|_Value
    :param right:
    :type right: _Column|_Value
    """

    def __init__(self, left, right):
        super().__init__(
            left=left,
            right=right,
            operator=operators._Join(how="LEFT"),
        )


class RightJoin(lineage._Expression):

    """
    :param left:
    :type right: _Column|_Value
    :param right:
    :type right: _Column|_Value
    """

    def __init__(self, left, right):
        super().__init__(
            left=left,
            right=right,
            operator=operators._Join(how="RIGHT"),
        )


class InnerJoin(lineage._Expression):

    """
    :param left:
    :type right: _Column|_Value
    :param right:
    :type right: _Column|_Value
    """

    def __init__(self, left, right):
        super().__init__(
            left=left,
            right=right,
            operator=operators._Join(how="INNER"),
        )


class FullOuterJoin(lineage._Expression):

    """
    :param left:
    :type right: _Column|_Value
    :param right:
    :type right: _Column|_Value
    """

    def __init__(self, left, right):
        super().__init__(
            left=left,
            right=right,
            operator=operators._Join(how="FULL OUTER"),
        )


class And(lineage._Expression):

    """
    :param left:
    :type right: _Column|_Value
    :param right:
    :type right: _Column|_Value
    """

    def __init__(self, left, right):
        super().__init__(left=left, right=right, operator=operators.And())


class Or(lineage._Expression):

    """
    :param left:
    :type right: _Column|_Value
    :param right:
    :type right: _Column|_Value
    """

    def __init__(self, left, right):
        super().__init__(left=left, right=right, operator=operators.Or())


class Between(lineage._Expression):

    """
    :param left:
    :type right: _Column|_Value
    :param right:
    :type right: _Column|_Value
    """

    def __init__(self, left, right: And):
        super().__init__(
            left=left,
            right=right,
            operator=operators._Between(),
        )
