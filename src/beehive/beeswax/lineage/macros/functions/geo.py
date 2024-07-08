from ....lineage import core as lineage
from ....lineage import columns as lineage_columns
from ....lineage import expressions as lineage_expressions
from ....lineage import functions as lineage_functions
from ....lineage import values as lineage_values
from ....lineage import tables as lineage_tables
from ....lineage import aggregates as lineage_aggregates
from ....lineage import combinations as lineage_combinations
from ....lineage import operators as lineage_operators
import pandas as pd

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

    latitude = lineage_functions.Round(
        lineage_functions.Degrees(
            lineage_functions.ASin(
                lineage_functions.Add(
                    lineage_functions.Multiply(
                        lineage_functions.Sin(
                            lineage_functions.Radians(
                                lineage_functions.ListExtract(
                                    origin,
                                    lineage_values.List([lineage_values.Integer(1)]),
                                )
                            )
                        ),
                        lineage_functions.Cos(
                            lineage_functions.Divide(distance, radius)
                        ),
                    ),
                    lineage_functions.Multiply(
                        lineage_functions.Multiply(
                            lineage_functions.Cos(
                                lineage_functions.Radians(
                                    lineage_functions.ListExtract(
                                        origin,
                                        lineage_values.List(
                                            [lineage_values.Integer(1)]
                                        ),
                                    )
                                )
                            ),
                            lineage_functions.Sin(
                                lineage_functions.Divide(distance, radius)
                            ),
                        ),
                        lineage_functions.Cos(lineage_functions.Radians(bearing)),
                    ),
                )
            )
        ),
        lineage_values.Integer(6),
    )

    longitude = lineage_functions.Round(
        lineage_functions.Degrees(
            lineage_functions.Add(
                lineage_functions.Radians(
                    lineage_functions.ListExtract(
                        origin, lineage_values.List([lineage_values.Integer(2)])
                    )
                ),
                lineage_functions.ATan2(
                    lineage_functions.Multiply(
                        lineage_functions.Multiply(
                            lineage_functions.Sin(lineage_functions.Radians(bearing)),
                            lineage_functions.Sin(
                                lineage_functions.Divide(distance, radius)
                            ),
                        ),
                        lineage_functions.Cos(
                            lineage_functions.Radians(
                                lineage_functions.ListExtract(
                                    origin,
                                    lineage_values.List([lineage_values.Integer(1)]),
                                )
                            )
                        ),
                    ),
                    lineage_functions.Subtract(
                        lineage_functions.Cos(
                            lineage_functions.Divide(distance, radius)
                        ),
                        lineage_functions.Multiply(
                            lineage_functions.Sin(
                                lineage_functions.ListExtract(
                                    origin,
                                    lineage_values.List([lineage_values.Integer(1)]),
                                )
                            ),
                            lineage_functions.Cos(latitude),
                        ),
                    ),
                ),
            )
        ),
        lineage_values.Integer(6),
    )

    return lineage_values.List([latitude, longitude])


def as_list(source):
    return lineage_functions.Cast(
        lineage_functions.RegExpExtractAll(
            lineage_functions.GeoAsWKT(source), lineage_values.Varchar(r"[\d\.]+")
        ),
        lineage_values.Datatype("FLOAT[]"),
    )
