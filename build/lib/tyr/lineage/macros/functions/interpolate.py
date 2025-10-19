from ... import core as lineage
from ... import values as lineage_values
from ... import functions as lineage_functions


def linear(start, end, delta: lineage._Column, max_delta: lineage._Column):
    return lineage_functions.math.Subtract(
        left=start,
        right=lineage_functions.math.Multiply(
            left=lineage_functions.math.Subtract(left=start, right=end),
            right=lineage_functions.math.Divide(
                left=lineage_functions.math.Subtract(
                    left=max_delta,
                    right=delta,
                ),
                right=max_delta,
            ),
        ),
    )


def quadratic(start, end, delta: lineage._Column, max_delta: lineage._Column):
    return lineage_functions.math.Subtract(
        left=start,
        right=lineage_functions.math.Multiply(
            left=lineage_functions.math.Subtract(left=start, right=end),
            right=lineage_functions.math.Exponent(
                source=lineage_functions.math.Divide(
                    left=lineage_functions.math.Subtract(
                        left=max_delta,
                        right=delta,
                    ),
                    right=max_delta,
                ),
                exponent=lineage_values.Integer(2),
            ),
        ),
    )
