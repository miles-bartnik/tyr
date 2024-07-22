from .... import lineage
import units


def _fahrenheit_rankine(source):
    return lineage.functions.math.Add(
        source._override_unit(units.core.Unit("°R^1")),
        lineage.values.Float(459.67, unit=units.core.Unit("°R^1")),
    )


def _rankine_fahrenheit(source):
    return lineage.functions.math.Subtract(
        source._override_unit(units.core.Unit("°F^1")),
        lineage.values.Float(459.67, units.core.Unit("°F^1")),
    )


def _celcius_kelvin(source):
    return lineage.functions.math.Add(
        source._override_unit(units.core.Unit("K^1")),
        lineage.values.Float(273.15, unit=units.core.Unit("K^1")),
    )


def _kelvin_celcius(source):
    return lineage.functions.math.Subtract(
        source._override_unit(units.core.Unit("°C^1")),
        lineage.values.Float(273.15, unit=units.core.Unit("°C^1")),
    )


def _rankine_kelvin(source):
    return lineage.functions.math.Multiply(
        source._override_unit(units.core.Unit("K^1")),
        lineage.functions.math.Divide(
            lineage.values.Float(5, unit=units.core.Unit("K^1")),
            lineage.values.Float(9, unit=units.core.Unit("K^1")),
        ),
    )


def _kelvin_rankine(source):
    return lineage.functions.math.Multiply(
        source._override_unit(units.core.Unit("°R^1")),
        lineage.functions.math.Divide(
            lineage.values.Float(9, unit=units.core.Unit("°R^1")),
            lineage.values.Float(5, unit=units.core.Unit("°R^1")),
        ),
    )


def convert_to_unit(source, unit: units.core.Unit):
    if not source.unit:
        raise AttributeError("source has no unit assigned")

    if source.unit.name == unit.name:
        return source
    elif source.unit.name == "°F^1":
        if unit.name == "°C^1":
            return _kelvin_celcius(_rankine_kelvin(_fahrenheit_rankine(source)))
        elif unit.name == "°R^1":
            return _fahrenheit_rankine(source)
        elif unit.name == "K^1":
            return _rankine_kelvin(_fahrenheit_rankine(source))
    elif source.unit.name == "°C^1":
        if unit.name == "°F^1":
            return _rankine_fahrenheit(_kelvin_rankine(_celcius_kelvin(source)))
        elif unit.name == "°R^1":
            return _kelvin_rankine(_celcius_kelvin(source))
        elif unit.name == "K^1":
            return _celcius_kelvin(source)
    elif source.unit.name == "°R^1":
        if unit.name == "°F^1":
            return _rankine_fahrenheit(source)
        elif unit.name == "°C^1":
            return _kelvin_celcius(_rankine_kelvin(source))
        elif unit.name == "K^1":
            return _rankine_kelvin(source)
    elif source.unit.name == "K^1":
        if unit.name == "°F^1":
            return _rankine_fahrenheit(_kelvin_rankine(source))
        elif unit.name == "°C^1":
            return _kelvin_celcius(source)
        elif unit.name == "°R^1":
            return _kelvin_rankine(source)
    else:
        conversion_factor, unit = units.core.convert_unit_value(
            1, source_unit=source.unit, target_unit=unit
        )

        if conversion_factor != 1:
            return lineage.functions.data_type.Cast(
                lineage.functions.math.Multiply(
                    left=source,
                    right=lineage.functions.data_type.Cast(
                        lineage.values.Float(
                            conversion_factor,
                            unit=units.core.Unit(
                                source.unit.reciprocal().name + unit.name
                            ),
                        ),
                        data_type=lineage.values.Datatype("FLOAT8"),
                    ),
                ),
                source.data_type,
            )

        else:
            return lineage.functions.data_type.Cast(
                source,
                source.data_type,
            )


def convert_to_si(source):
    if not source.unit:
        raise AttributeError("source has no unit assigned")

    conversion_factor, si_equivalent = units.core.value_to_si(1, unit=source.unit)

    if source.unit.name == si_equivalent.name:
        return source
    elif source.unit.name == "°F^1":
        return _rankine_kelvin(_fahrenheit_rankine(source))
    elif source.unit.name == "°C^1":
        return _celcius_kelvin(source)
    elif source.unit.name == "°R^1":
        return _rankine_kelvin(source)
    elif source.unit.name == "K^1":
        return source
    else:
        return lineage.functions.data_type.Cast(
            lineage.functions.math.Multiply(
                left=source,
                right=lineage.functions.data_type.Cast(
                    lineage.values.Float(
                        conversion_factor,
                        unit=units.core.Unit(
                            source.unit.reciprocal().name + si_equivalent.name
                        ),
                    ),
                    data_type=lineage.values.Datatype("FLOAT8"),
                ),
            ),
            source.data_type,
        )
