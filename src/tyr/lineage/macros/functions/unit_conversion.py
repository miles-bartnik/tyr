from tyr import lineage
import units

# NEEDS COMPLETE REBUILD


# Something strange is happening in line 15-30 where target unit is not being applied
def convert_to_unit(source, target_unit: units.core.Unit):
    source_unit = source.unit

    conversion_plan = units.core.get_conversion_plan(source_unit, target_unit)

    if conversion_plan.empty:
        return source

    conversion = source

    for index, row in conversion_plan.iterrows():
        apply_prefix = row["apply_prefix"]
        apply_conversion = row["apply_conversion"]

        conversion_factor_unit = units.core.multiply(
            units.core.Unit(row["source_unit"]).reciprocal(),
            units.core.Unit(row["target_unit"]),
        )

        if apply_prefix != 1:
            conversion = lineage.functions.math.Multiply(
                conversion,
                lineage.functions.math.Round(
                    lineage.values.Float(apply_prefix), lineage.values.Integer(5)
                ),
            )

        conversion = lineage.functions.math.Multiply(
            conversion,
            lineage.functions.math.Round(
                lineage.values.Float(apply_conversion, unit=conversion_factor_unit),
                lineage.values.Integer(5),
            ),
        )

    return conversion
