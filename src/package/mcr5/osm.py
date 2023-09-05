import pandas as pd
from osmnx.distance import cKDTree


def add_nearest_osm_node_id(
    geo_count_df: pd.DataFrame, osm_nodes_df: pd.DataFrame
) -> pd.DataFrame:
    """
    Find the nearest OSM node for each GeoCount and return a dict with the
    mapping from OSM node ID to count.
    """
    osm_nodes_array = osm_nodes_df[["lat", "lon"]].to_numpy()
    kdtree = cKDTree(osm_nodes_array)

    to_query = geo_count_df[["lat", "lon"]].to_numpy()
    distance, index = kdtree.query(to_query)

    distance = distance * 111111
    nearest_node_ids = osm_nodes_df.iloc[index].id.values

    geo_count_df["nearest_osm_node_id"] = nearest_node_ids
    geo_count_df["distance"] = distance

    if geo_count_df["distance"].max() > 1000:
        print("WARNING: max distance > 1000m")

    return geo_count_df
