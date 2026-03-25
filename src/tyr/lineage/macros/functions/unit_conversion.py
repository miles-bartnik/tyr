from ... import functions as lineage_functions
from ... import values as lineage_values
from ...units.core import (
    Unit,
    multiply as unit_multiply,
    get_conversion_plan as unit_get_conversion_plan,
)

# NEEDS COMPLETE REBUILD


# Something strange is happening in line 15-30 where target unit is not being applied
def convert_to_unit(source, target_unit: Unit):
    source_unit = source.unit

    conversion_plan = unit_get_conversion_plan(source_unit, target_unit)

    if conversion_plan.empty:
        return source

    conversion = source

    for index, row in conversion_plan.iterrows():
        apply_prefix = row["apply_prefix"]
        apply_conversion = row["apply_conversion"]

        conversion_factor_unit = unit_multiply(
            Unit(row["source_unit"]).reciprocal(),
            Unit(row["target_unit"]),
        )

        if apply_prefix != 1:
            conversion = lineage_functions.math.Multiply(
                conversion,
                lineage_functions.math.Round(
                    lineage_values.FloatingPoint(apply_prefix),
                    lineage_values.Integer(5),
                ),
            )

        conversion = lineage_functions.math.Multiply(
            conversion,
            lineage_functions.math.Round(
                lineage_values.FloatingPoint(
                    apply_conversion, unit=conversion_factor_unit
                ),
                lineage_values.Integer(5),
            ),
        )

    return conversion
