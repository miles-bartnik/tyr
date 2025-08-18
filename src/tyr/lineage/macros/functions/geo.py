from ... import functions as lineage_functions
from ... import values as lineage_values

EARTH_RADIUS = 6371.009


def point_from_distance_bearing(
    origin,
    bearing: lineage_values.Float,
    distance: lineage_values.Float,
    radius: lineage_values.Float = lineage_values.Float(EARTH_RADIUS),
):
    if origin.data_type.value == "FLOAT[]":
        origin = origin

    elif origin.data_type.value == "GEOMETRY":
        origin = origin

    else:
        print(origin.data_type.__dict__)
        raise ValueError("origin must be either FLOAT[] or GEOMETRY")

    latitude = lineage_functions.math.Round(
        lineage_functions.math.Degrees(
            lineage_functions.math.ASin(
                lineage_functions.math.Add(
                    lineage_functions.math.Multiply(
                        lineage_functions.math.Sin(
                            lineage_functions.math.Radians(
                                lineage_functions.ListExtract(
                                    origin,
                                    lineage_values.List([lineage_values.Integer(1)]),
                                )
                            )
                        ),
                        lineage_functions.math.Cos(
                            lineage_functions.math.Divide(distance, radius)
                        ),
                    ),
                    lineage_functions.math.Multiply(
                        lineage_functions.math.Multiply(
                            lineage_functions.math.Cos(
                                lineage_functions.math.Radians(
                                    lineage_functions.ListExtract(
                                        origin,
                                        lineage_values.List(
                                            [lineage_values.Integer(1)]
                                        ),
                                    )
                                )
                            ),
                            lineage_functions.math.Sin(
                                lineage_functions.math.Divide(distance, radius)
                            ),
                        ),
                        lineage_functions.math.Cos(
                            lineage_functions.math.Radians(bearing)
                        ),
                    ),
                )
            )
        ),
        lineage_values.Integer(6),
    )

    longitude = lineage_functions.math.Round(
        lineage_functions.math.Degrees(
            lineage_functions.math.Add(
                lineage_functions.math.Radians(
                    tyr.lineage.functions.array.ListExtract(
                        origin, lineage_values.List([lineage_values.Integer(2)])
                    )
                ),
                lineage_functions.math.ATan2(
                    lineage_functions.math.Multiply(
                        lineage_functions.math.Multiply(
                            lineage_functions.math.Sin(
                                lineage_functions.math.Radians(bearing)
                            ),
                            lineage_functions.math.Sin(
                                lineage_functions.math.Divide(distance, radius)
                            ),
                        ),
                        lineage_functions.math.Cos(
                            lineage_functions.math.Radians(
                                tyr.lineage.functions.array.ListExtract(
                                    origin,
                                    lineage_values.List([lineage_values.Integer(1)]),
                                )
                            )
                        ),
                    ),
                    lineage_functions.math.Subtract(
                        lineage_functions.math.Cos(
                            lineage_functions.math.Divide(distance, radius)
                        ),
                        lineage_functions.math.Multiply(
                            lineage_functions.math.Sin(
                                tyr.lineage.functions.array.ListExtract(
                                    origin,
                                    lineage_values.List([lineage_values.Integer(1)]),
                                )
                            ),
                            lineage_functions.math.Cos(latitude),
                        ),
                    ),
                ),
            )
        ),
        lineage_values.Integer(6),
    )

    return lineage_values.List([latitude, longitude])


def as_list(source):
    return tyr.lineage.functions.data_type.Cast(
        lineage_functions.string.RegExpExtractAll(
            lineage_functions.geo.AsWKT(source),
            lineage_values.Varchar(r"[\d\.]+"),
        ),
        lineage_values.Datatype("FLOAT[]"),
    )
