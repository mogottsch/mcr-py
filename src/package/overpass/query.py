import os
import textwrap
from typing import Any
import overpy
import pandas as pd
import geopandas as gpd
from concurrent.futures import ThreadPoolExecutor
from joblib import Memory

from shapely.geometry import Polygon
from package.key import TMP_DIR_LOCATION
from package.logger import rlog


api = overpy.Overpass()


def build(attr: list[tuple], bounding_box: tuple[float, float, float, float]) -> str:
    bounding_box = (bounding_box[1], bounding_box[0], bounding_box[3], bounding_box[2])
    query = textwrap.dedent(
        f"""
        [out:json];
        """
    )
    for category, feature in attr:
        query += textwrap.dedent(
            f"""
            (
                node["{category}"="{feature}"]{bounding_box};
                way["{category}"="{feature}"]{bounding_box};
                relation[{category}="{feature}"]{bounding_box};
            );
            out center;
            """
        )
    return query


memory = Memory(os.path.join(TMP_DIR_LOCATION, "overpy"), verbose=0)


@memory.cache
def query(query: str):
    result = api.query(query)

    def get_name(obj) -> str:
        try_attributes = ["name", "shop", "amenity", "leisure"]
        name = None
        for attr in try_attributes:
            if attr in obj.tags:
                name = obj.tags[attr]
                break
        if name is None:
            rlog.warning(f"Could not find name for {obj}")
            name = "Unknown"
        return name

    pois = []
    for obj in result.nodes + result.ways + result.relations:
        poi = {
            "name": get_name(obj),
            "id": obj.id,
        }
        if isinstance(obj, overpy.Node):
            poi["lat"] = obj.lat
            poi["lon"] = obj.lon
        elif isinstance(obj, overpy.Way):
            poi["lat"] = obj.center_lat
            poi["lon"] = obj.center_lon
        elif isinstance(obj, overpy.Relation):
            poi["lat"] = obj.center_lat
            poi["lon"] = obj.center_lon
        else:
            raise ValueError(f"Unknown type: {type(obj)}")
        pois.append(poi)

    if len(pois) == 0:
        print("No POIs found")
        return None

    pois = pd.DataFrame(pois)

    pois = gpd.GeoDataFrame(pois, geometry=gpd.points_from_xy(pois.lon, pois.lat))
    return pois


def fetch_and_merge_queries_async(
    queries: list[tuple[str, str]], area_of_interest: Polygon
) -> gpd.GeoDataFrame:
    def fetch_data(name_query_pair: tuple[str, str]) -> tuple[str, Any]:
        name, query_str = name_query_pair
        return (name, query(query_str))

    with ThreadPoolExecutor() as executor:
        poi_groups = list(executor.map(fetch_data, queries))

    for name, pois in poi_groups:
        pois["type"] = name
    pois: gpd.GeoDataFrame = pd.concat([pois for _, pois in poi_groups])  # type: ignore
    pois: gpd.GeoDataFrame = pois[pois.intersects(area_of_interest)]  # type: ignore

    return pois
