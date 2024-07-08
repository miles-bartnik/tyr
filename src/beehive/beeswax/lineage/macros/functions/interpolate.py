from ....lineage import core as lineage
from ....lineage import columns as lineage_columns
from ....lineage import expressions as lineage_expressions
from ....lineage import functions as lineage_functions
from ....lineage import values as lineage_values
from ....lineage import tables as lineage_tables
from ....lineage import aggregates as lineage_aggregates
from ....lineage import combinations as lineage_combinations
from ....lineage import operators as lineage_operators


def linear(start, end, delta: lineage._Column, max_delta: lineage._Column):
    return lineage_functions.Subtract(
        left=start,
        right=lineage_functions.Multiply(
            left=lineage_functions.Subtract(left=start, right=end),
            right=lineage_functions.Divide(
                left=lineage_functions.Subtract(
                    left=max_delta,
                    right=delta,
                ),
                right=max_delta,
            ),
        ),
    )


def quadratic(start, end, delta: lineage._Column, max_delta: lineage._Column):
    return lineage_functions.Subtract(
        left=start,
        right=lineage_functions.Multiply(
            left=lineage_functions.Subtract(left=start, right=end),
            right=lineage_functions.Exponent(
                source=lineage_functions.Divide(
                    left=lineage_functions.Subtract(
                        left=max_delta,
                        right=delta,
                    ),
                    right=max_delta,
                ),
                exponent=lineage_values.Integer(2),
            ),
        ),
    )
