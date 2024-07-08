import h3
from geopy.distance import distance
import shapely
import math
import numpy as np
import plotly.graph_objects as go
from typing import Tuple, List

r_earth = 6371

"""
All of this needs to be reworked into a hp macros module
"""


class GeoCoordinate:
    def __init__(self, latitude: float, longitude: float):
        a = 6378388
        b = a * (1 - 1 / 297)

        if not (-90 <= latitude <= 90):
            raise ValueError(rf"Invalid latitude: {latitude}")

        if not (-180 <= longitude <= 180):
            raise ValueError(rf"Invalid longitude: {longitude}")

        self.latitude = latitude
        self.longitude = longitude

        self.geo_coordinate = shapely.Point(latitude, longitude)

        if longitude < 0:
            bearing = 360 + longitude
        else:
            bearing = latitude

        pitch = latitude

        self.polar_coordinate = shapely.Point(pitch, bearing)


class PolarCoordinate:
    def __init__(self, bearing: float, pitch: float):
        if not (-math.pi / 2 <= pitch <= math.pi / 2):
            raise ValueError(rf"Invalid pitch: {pitch}")

        if not (0 <= bearing <= 2 * math.pi):
            raise ValueError(rf"Invalid bearing: {bearing}")

        self.coordinate = shapely.Point(pitch, bearing)
        self.bearing = bearing
        self.pitch = pitch


class CartesianCoordinate:
    def __init__(self, x: float, y: float, z: float):
        self.coordinate = shapely.Point(x, y, z)
        self.x = x
        self.y = y
        self.z = z
        self.array = np.array([x, y, z])


def point_from_distance_bearing(point: GeoCoordinate, dist: float, bearing: float):
    """
    Args:
    latitude(float): latitude of starting point
    longitude(float): longitude of starting point
    dist(float): km to destination
    bearing(float): bearing to destination

    Returns:
    Tuple of (longitude, latitude)
    """

    result = distance(kilometers=dist).destination(
        point=(point.latitude, point.longitude), bearing=bearing
    )

    return GeoCoordinate(latitude=result.latitude, longitude=result.longitude)


def polar_to_geo(point: PolarCoordinate):
    if math.degrees(point.bearing) > 180:
        longitude = math.degrees(point.bearing) - 360
    else:
        longitude = math.degrees(point.bearing)

    latitude = math.degrees(point.pitch)

    return GeoCoordinate(latitude, longitude)


def geo_to_polar(point: GeoCoordinate):
    bearing = math.radians(point.longitude)
    pitch = math.radians(point.latitude)

    if bearing < 0:
        bearing = 2 * math.pi + bearing

    return PolarCoordinate(bearing, pitch)


def hex_to_poly(hexgrid: str):
    # h3.h3_set_to_multi_polygon returns (pitch, bearing) but Polygon requires (bearing, pitch) so the list comprehension is required here
    return shapely.Polygon(
        [
            (item[1], item[0])
            for item in h3.h3_set_to_multi_polygon([hexgrid], geo_json=False)[0][0]
        ]
    )


def poly_to_hexgrids(polygon: shapely.Polygon, resolution: int = 15):
    hexagons = list(
        h3.polyfill(polygon.__geo_interface__, resolution, geo_json_conformant=True)
    )

    return hexagons


def reduce_hex_resolution(hex_id: str, resolution: int):
    new_hex_id = hex_id

    if h3.h3_get_resolution(hex_id) - resolution <= 0:
        raise ValueError(
            rf"Invalid resolution value range of {h3.h3_get_resolution(hex_id) - resolution}"
        )

    for i in range(h3.h3_get_resolution(hex_id) - resolution):
        new_hex_id = h3.h3_to_parent(new_hex_id)

    return new_hex_id


def polar_to_cartesian(point: PolarCoordinate, r):
    x = r * math.cos(point.bearing) * math.cos(point.pitch)
    y = r * math.sin(point.bearing) * math.cos(point.pitch)
    z = r * math.sin(point.pitch)

    return CartesianCoordinate(x, y, z)


def cartesian_to_polar(point: CartesianCoordinate, r=None):
    if np.isnan([point.x, point.y, point.z]).any():
        return (np.nan, np.nan)

    if not r:
        r = math.sqrt(point.x**2 + point.y**2 + point.z**2)

    if point.z == 0:
        pitch = 0

    elif point.z == r:
        pitch = math.pi / 2
    elif point.z == -r:
        pitch = -math.pi / 2

    else:
        pitch = math.atan(point.z / math.sqrt((point.x**2 + point.y**2)))

    if point.x == 0:
        if point.y > 0:
            bearing = math.pi / 2

        elif point.y < 0:
            bearing = 3 * math.pi / 2

        elif point.y == 0:
            bearing = 0

    elif point.x > 0:
        if point.y > 0:
            bearing = math.atan(abs(point.y) / abs(point.x))

        elif point.y < 0:
            bearing = 2 * math.pi - math.atan(abs(point.y) / abs(point.x))

        elif point.y == 0:
            bearing = 0

    elif point.x < 0:
        if point.y > 0:
            bearing = math.pi / 2 + math.atan(abs(point.x) / abs(point.y))

        elif point.y < 0:
            bearing = math.pi + math.atan(abs(point.y) / abs(point.x))

        elif point.y == 0:
            bearing = math.pi

    return PolarCoordinate(bearing, pitch)


def sphere_coords(x=0, y=0, z=0, r=r_earth, resolution=100, polygon=None):
    if polygon:
        (minx, miny, maxx, maxy) = polygon.bounds
        minx = math.radians(minx)
        miny = math.radians(miny) + math.pi / 2
        maxx = math.radians(maxx)
        maxy = math.radians(maxy) + math.pi / 2
    else:
        minx = 0
        maxx = 2 * math.pi
        miny = 0
        maxy = math.pi

    U, V = np.mgrid[minx : maxx : resolution * 1j, miny : maxy : resolution * 1j]

    X = np.zeros((resolution, resolution))
    Y = np.zeros((resolution, resolution))
    Z = np.zeros((resolution, resolution))

    for u in range(len(U)):
        for v in range(len(V)):
            if polygon:
                if polygon.contains(
                    shapely.Point(
                        math.degrees(U[u][v]), math.degrees(V[u][v] - math.pi / 2)
                    )
                ):
                    X[u][v] = r * np.cos(U[u][v]) * np.sin(V[u][v]) + x
                    Y[u][v] = r * np.sin(U[u][v]) * np.sin(V[u][v]) + y
                    Z[u][v] = -r * np.cos(V[u][v]) + z
                else:
                    X[u][v] = 0
                    Y[u][v] = 0
                    Z[u][v] = 0
            else:
                X[u][v] = r * np.cos(U[u][v]) * np.sin(V[u][v]) + x
                Y[u][v] = r * np.sin(U[u][v]) * np.sin(V[u][v]) + y
                Z[u][v] = -r * np.cos(V[u][v]) + z

    return X, Y, Z


def sphere_mesh(X, Y, Z, color):
    color = rf"rgb({color[0]},{color[1]},{color[2]})"

    sphere_mesh = go.Surface(
        x=X, y=Y, z=Z, opacity=1, colorscale=[[0, color], [1, color]]
    )

    return sphere_mesh


def geojson_feature_to_polygon(feature):
    return shapely.Polygon(shapely.geometry.shape(feature["geometry"]))


def radial_polys(
    centre: shapely.Point,
    angle_delta: int,
    distance_delta: int,
    max_distance: int = None,
):
    if 360 % angle_delta != 0:
        print(
            "Warning: Angle delta value does not divide 360 completely. Radial polygons may not be created correctly"
        )

    angles = range(0, 360, angle_delta)

    distances = (
        [i for i in range(0, 20, distance_delta)]
        + [i for i in range(20, 50, 2 * distance_delta)]
        + [i for i in range(50, 100, 5 * distance_delta)]
        + [i for i in range(100, 200, 10 * distance_delta)]
    )

    if max_distance:
        distances = [i for i in distances if i <= max_distance]

    polys = []

    for i in range(len(angles) - 1):
        for j in range(len(distances) - 1):
            coords = [
                point_from_distance_bearing(
                    centre.coords.xy[1][0],
                    centre.coords.xy[0][0],
                    distances[j],
                    angles[i],
                ),
                point_from_distance_bearing(
                    centre.coords.xy[1][0],
                    centre.coords.xy[0][0],
                    distances[j],
                    angles[i + 1],
                ),
                point_from_distance_bearing(
                    centre.coords.xy[1][0],
                    centre.coords.xy[0][0],
                    distances[j + 1],
                    angles[i + 1],
                ),
                point_from_distance_bearing(
                    centre.coords.xy[1][0],
                    centre.coords.xy[0][0],
                    distances[j + 1],
                    angles[i],
                ),
            ]

            polys.append(shapely.Polygon(coords))

    return polys


def poly_coords(polygon, r=r_earth):
    X = [0]
    Y = [0]
    Z = [0]

    for coord in polygon.__geo_interface__["coordinates"][0]:
        (x, y, z) = polar_to_cartesian(coord[0], coord[1], r)

        X.append(x)
        Y.append(y)
        Z.append(z)

    return X, Y, Z


def rotate_cartesian(v: CartesianCoordinate, k: CartesianCoordinate, theta):
    value = (
        v.array * math.cos(theta)
        + np.cross(
            k.array / np.linalg.norm(k.array),
            v.array,
        )
        * math.sin(theta)
        + k.array
        / np.linalg.norm(k.array)
        * (
            np.dot(
                k.array / np.linalg.norm(k.array),
                v.array,
            )
        )
        * (1 - math.cos(theta))
    )

    return CartesianCoordinate(value[0], value[1], value[2])


def scale_cartesian(
    origin: CartesianCoordinate, target: CartesianCoordinate, factor: float
):
    normal = np.cross(origin.array, target.array)

    theta = np.arccos(
        np.clip(
            np.dot(
                origin.array / np.linalg.norm(origin.array),
                target.array / np.linalg.norm(target.array),
            ),
            -1.0,
            1.0,
        )
    )

    return rotate_cartesian(
        v=target,
        k=CartesianCoordinate(normal[0], normal[1], normal[2]),
        theta=(theta * (factor - 1)),
    )


def mirror_polar(target: PolarCoordinate, origin: PolarCoordinate, axis: str):
    diff = {
        "bearing": target.bearing - origin.bearing,
        "pitch": target.pitch - origin.pitch,
    }

    if axis == "pitch":
        diff = {"bearing": diff["bearing"], "pitch": -diff["pitch"]}

    elif axis == "bearing":
        diff = {"bearing": -diff["bearing"], "pitch": diff["pitch"]}

    elif axis == "both":
        diff = {"bearing": -diff["bearing"], "pitch": -diff["pitch"]}

    else:
        raise ValueError("axis must be in ['pitch', 'bearing', 'both']")

    result = PolarCoordinate(
        bearing=origin.bearing + diff["bearing"], pitch=origin.pitch + diff["pitch"]
    )

    return result
