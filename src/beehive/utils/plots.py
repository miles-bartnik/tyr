import math
import folium

# from folium import plugins
import geopandas as gpd
import json
import numpy as np
from typing import List, Dict
import pandas as pd

# from .utils import get_receivers
# from .mappings import folium_time_deltas
# from .geo import point_from_distance_bearing, point_to_hex, hex_to_poly
import shapely

folium_time_deltas = {
    "day": {"period": "PT1D", "duration": "PT23H"},
    "hour": {"period": "PT1H", "duration": "PT59M"},
    "minute": {"period": "PT1M", "duration": "PT59S"},
}


def get_poly_data(df: pd.DataFrame, polys: List[shapely.Polygon], columns: Dict):
    geo_polys = []

    for poly in polys:
        weight_df = df[(poly.contains(df["point"]))]

        if not weight_df.empty and len(weight_df) > 0:
            new_poly = {"geometry": poly}

            for key in columns.keys():
                if columns[key] == "first":
                    new_poly[key] = weight_df[key].unique()[0]
                else:
                    new_poly[key] = weight_df[key].aggregate(columns[key])

            geo_polys.append(new_poly)

    geo_polys = pd.DataFrame.from_records(geo_polys).dropna(how="any")
    geo_polys["id"] = geo_polys.index.tolist()

    geo_polys = gpd.GeoDataFrame(geo_polys)
    geo_polys.crs = "EPSG:4236"

    return geo_polys


def polygon_plot(
    geo_polys: gpd.GeoDataFrame,
    m: folium.Map,
    column: str,
    bins: List[int],
    group_by: str = None,
    filepath: str = None,
    fill_color: str = "GnBu",
):
    if group_by:
        for value in geo_polys[group_by].unique():
            cp = folium.Choropleth(
                geo_data=geo_polys[geo_polys[group_by] == value][
                    "geometry"
                ].__geo_interface__,
                data=geo_polys[geo_polys[group_by] == value],
                columns=["id", column],
                legend_name=column,
                bins=bins,
                key_on="feature.id",
                fill_color=fill_color,
                line_opacity=0.1,
                name=value,
            )

            for key in cp._children:
                if (key.startswith("color_map")) and (
                    value != geo_polys[group_by].unique()[0]
                ):
                    del cp._children[key]

            m.add_child(cp)

            for s in cp.geojson.data["features"]:
                s["properties"][group_by] = geo_polys.loc[int(s["id"]), group_by]
                s["properties"][column] = int(geo_polys.loc[int(s["id"]), column])

            folium.GeoJsonTooltip(
                list(cp.geojson.data["features"][0]["properties"].keys())
            ).add_to(cp.geojson)

    else:
        cp = folium.Choropleth(
            geo_data=geo_polys["geometry"].__geo_interface__,
            data=geo_polys,
            columns=["id", column],
            legend_name=column,
            bins=bins,
            key_on="feature.id",
            fill_color=fill_color,
            line_opacity=0.1,
        )

        m.add_child(cp)

        for s in cp.geojson.data["features"]:
            s["properties"][column] = int(geo_polys.loc[int(s["id"]), column])

        folium.GeoJsonTooltip(
            list(cp.geojson.data["features"][0]["properties"].keys())
        ).add_to(cp.geojson)

    m.add_child(folium.map.LayerControl())

    if filepath:
        print(rf"Saving file to {filepath}")
        m.save(filepath)
    else:
        return m


# def hex_bin_plot(geo_polys:gpd.GeoDataFrame, filepath:str=None):

#     m = folium.Map()

#     for receiver in geo_polys.receiver.unique():

#         bins = np.linspace(0, int(geo_polys['log_count_messages'].max())+1, 10)

#         cp = folium.Choropleth(geo_data=geo_polys[geo_polys['receiver']==receiver]['geometry'].__geo_interface__
#                         , data=geo_polys[geo_polys['receiver']==receiver]
#                         , columns=['id','log_count_messages']
#                         , legend_name = 'log_count_messages'
#                         , key_on="feature.id"
#                         , bins=bins
#                         , fill_color='GnBu'
#                         , line_opacity=0.1
#                         , name=receiver
#                         )

#         for key in cp._children:
#             if (key.startswith('color_map')) and (receiver != geo_polys.receiver.unique()[0]):
#                 del(cp._children[key])

#         m.add_child(cp)

#         for s in cp.geojson.data['features']:
#             s['properties']['class'] = geo_polys.loc[int(s['id']), 'class']
#             s['properties']['receiver'] = geo_polys.loc[int(s['id']), 'receiver']
#             s['properties']['hex'] = geo_polys.loc[int(s['id']), 'hexgrid']
#             s['properties']['log_count_messages'] = format(round(geo_polys.loc[int(s['id']), 'log_count_messages'], 4), '.4f')
#             s['properties']['count_messages'] = int(geo_polys.loc[int(s['id']), 'count_messages'])
#             s['properties']['count_mmsi'] = int(geo_polys.loc[int(s['id']), 'count_mmsi'])
#             s['properties']['avg_interval_diff'] = format(round(geo_polys.loc[int(s['id']), 'avg_interval_diff'], 2), '.2f')

#         folium.GeoJsonTooltip(list(cp.geojson.data['features'][0]['properties'].keys())).add_to(cp.geojson)

#     m.add_child(folium.map.LayerControl())

#     if filepath:
#         print(rf"Saving file to {filepath}")
#         m.save(filepath)
#     else:
#         return m


# def hex_bin_plot_exp(geo_polys:gpd.GeoDataFrame, filepath:str=None):

#     m = folium.Map()

#     features = []

#     for receiver in geo_polys.receiver.unique():

#         features.append([
#                         {
#                             "type": "Feature",
#                             "geometry": {'type':'MultiPolygon','coordinates':[hex_to_poly(row['hexgrid']).__geo_interface__['coordinates']]},
#                             "properties": {
#                                 "times": [str(row["time"])],
#                                 "style": {
#                                     "color": row["count_messages"],
#                                 },
#                             },
#                         }
#                         for index, row in geo_polys[geo_polys['receiver']==receiver].iterrows()
#                     ])


#     tsgj = plugins.TimestampedGeoJson(
#                                     {
#                                         "type": "FeatureCollection",
#                                         "features": features,
#                                     },
#                                     period=rf"{folium_time_deltas[geo_polys['granularity'].unique()[0]]['period']}",
#                                     duration=rf"{folium_time_deltas[geo_polys['granularity'].unique()[0]]['duration']}",
#                                     add_last_point=False,
#                                     )

#     m.add_child(tsgj)

#     m.add_child(folium.map.LayerControl())

#     if filepath:
#         print(rf"Saving file to {filepath}")
#         m.save(filepath)
#     else:
#         return m
