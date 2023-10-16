from enum import Enum
import os
import shutil
from typing import Tuple
from typing_extensions import Annotated

import pyrosm
import pandas as pd
import geopandas as gpd
from shapely.geometry import MultiPoint
from pyrosm.data import get_data
from package import storage, cache
from package.geometa import GeoMeta
from package.osm import osm, key as osm_key
from package.logger import Timed, rlog
from package.osm import graph
from rich import print
from osmnx.distance import cKDTree


from package.logger import rlog
from package import console, key, storage

OSM_DIR_PATH = storage.get_tmp_path(key.TMP_OSM_DIR_NAME)


def list_available(selector: str):
    root_name = "Available OSM data"
    if selector != "":
        root_name += f" for {selector}"
    console.print_tree_from_any(get_available(selector), root_name=root_name)
    print(f"[i] Use one of the names listed above as value for `--city-id`.[/i]")


def get_available(
    selector: Annotated[str, "Selector in dot notation, e.g. '.regions.africa'"]
):
    data = pyrosm.data.available  # type: ignore
    if selector == "":
        return data

    # remove first . if present
    if len(selector) > 0 and selector[0] == ".":
        selector = selector[1:]
    keys = selector.split(".")
    for key in keys:
        if key not in data:
            raise ValueError(f"Key '{key}' not found in OSM data.")
        data = data[key]
    return data


def get_graph_for_city_cropped_to_boundary(
    osm_reader: pyrosm.OSM, geo_meta: GeoMeta
) -> Tuple[gpd.GeoDataFrame, gpd.GeoDataFrame]:
    hash = cache.combine_hashes(
        [
            cache.hash_str(osm_reader.filepath),
            geo_meta.hash(),
        ]
    )

    rlog.debug(f"Hash for OSM network: {hash}")

    if cache.cache_entry_exists(
        hash, osm_key.EDGES_FILE_IDENTIFIER
    ) and cache.cache_entry_exists(hash, osm_key.NODES_FILE_IDENTIFIER):
        rlog.info("Loading OSM network from cache")
        nodes = cache.read_gdf(hash, osm_key.NODES_FILE_IDENTIFIER)
        edges = cache.read_gdf(hash, osm_key.EDGES_FILE_IDENTIFIER)
        return nodes, edges

    with Timed.info("Reading OSM network"):
        # TODO: optimally we should first crop the data and then read the network
        nodes, edges = osm.read_network(osm_reader)

    with Timed.info("Cropping OSM network to boundary"):
        nodes = geo_meta.crop_gdf(nodes)
        edges = geo_meta.crop_gdf(edges)

    with Timed.info("Ensuring graph is connected"):
        nxgraph = graph.create_nx_graph(osm_reader, nodes, edges)
        n_nodes_before = len(nodes)
        nxgraph, nodes, edges = graph.crop_graph_to_largest_component(
            nxgraph, nodes, edges
        )
        n_nodes_after = len(nodes)
        rlog.info(
            f"Removed {n_nodes_before - n_nodes_after} nodes from OSM network to ensure connectivity ({n_nodes_before-n_nodes_after/n_nodes_before*100:.2f}%)"
        )

    with Timed.info("Caching OSM network"):
        cache.cache_gdf(nodes, hash, osm_key.NODES_FILE_IDENTIFIER)
        cache.cache_gdf(edges, hash, osm_key.EDGES_FILE_IDENTIFIER)

    return nodes, edges


def get_osm_reader_for_city_id_or_osm_path(city_id: str, osm_path: str) -> pyrosm.OSM:
    osm_path = osm_path if osm_path else osm.get_osm_path_from_city_id(city_id)

    if not os.path.exists(osm_path) and city_id:
        with Timed.info("Downloading OSM data"):
            osm.download_city(city_id, osm_path)
    else:
        rlog.info("Using existing OSM data")

    return new_osm_reader(osm_path)


def new_osm_reader(path: str, **kwargs) -> pyrosm.OSM:
    return pyrosm.OSM(path, **kwargs)


def read_network(osm_reader: pyrosm.OSM) -> Tuple[gpd.GeoDataFrame, gpd.GeoDataFrame]:
    network = osm_reader.get_network(nodes=True, network_type="walking")
    if network is None:
        raise Exception("No walking network found in OSM data.")
    nodes, edges = network
    return nodes, edges  # type: ignore


def crop_to_stops(
    nodes: gpd.GeoDataFrame, edges: gpd.GeoDataFrame, stops_df: gpd.GeoDataFrame
):
    combined_geometry = MultiPoint(stops_df.geometry.tolist())
    convex_hull = combined_geometry.convex_hull
    zone_of_interest = convex_hull.buffer(0.01)

    n_nodes_before = len(nodes)
    n_edges_before = len(edges)

    nodes = nodes.loc[nodes.geometry.within(zone_of_interest), :]
    edges = edges.loc[edges.u.isin(nodes.id) & edges.v.isin(nodes.id), :]
    rlog.info(
        f"{len(nodes)}/{n_nodes_before} ({len(nodes)/n_nodes_before*100:.2f}%) nodes remaining"
    )
    rlog.info(
        f"{len(edges)}/{n_edges_before} ({len(edges)/n_edges_before*100:.2f}%) edges remaining"
    )

    return nodes, edges


def crop_to_nodes(
    locations: gpd.GeoDataFrame, nodes: gpd.GeoDataFrame, buffer: float = 0
) -> gpd.GeoDataFrame:
    combined_geometry = MultiPoint(nodes.geometry.tolist())
    convex_hull = combined_geometry.convex_hull
    if buffer > 0:
        convex_hull = convex_hull.buffer(buffer)

    locations = locations.loc[locations.geometry.within(convex_hull), :]

    return locations


def get_osm_path_from_city_id(city_id: str) -> str:
    return os.path.join(OSM_DIR_PATH, f"{city_id}.pbf")


def download_city(city_id: str, path: str):
    fp = get_data(city_id, update=True)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    shutil.move(fp, path)


class OutlierMethod(Enum):
    REMOVE = "remove"
    WARN = "keep"


def add_nearest_osm_node_id(
    df_lat_long: pd.DataFrame,
    osm_nodes_df: pd.DataFrame,
    max_distance: float = 1000,
    outlier_method: OutlierMethod = OutlierMethod.WARN,
) -> pd.DataFrame | gpd.GeoDataFrame:
    """
    Adds the columns "nearest_osm_node_id" and "distance" to df_lat_long, which contains the
    nearest osm node id from osm_nodes_df and the distance to it for each entry in df_lat_long.

    Depending on the outlier_method, entries with a distance larger than max_distance are either
    removed or a warning is logged.

    Args:
        df_lat_long: The dataframe to assign to osm_nodes_df. Must contain columns "lat" and "lon".
        osm_nodes_df: The dataframe containing the osm nodes to assign df to. Must contain columns "lat" and "lon".
        max_distance: The maximum distance in meters to search for the nearest node.
    """
    osm_nodes_array = osm_nodes_df[["lat", "lon"]].to_numpy()
    kdtree = cKDTree(osm_nodes_array)

    to_query = df_lat_long[["lat", "lon"]].to_numpy()
    distance, index = kdtree.query(to_query)

    distance = distance * 111111
    nearest_node_ids = osm_nodes_df.iloc[index].id.values

    df_lat_long["nearest_osm_node_id"] = nearest_node_ids
    df_lat_long["distance"] = distance

    if df_lat_long["distance"].max() > max_distance:
        problematic_entries = df_lat_long.loc[df_lat_long["distance"] > max_distance]

        if outlier_method == OutlierMethod.REMOVE:
            df_lat_long = df_lat_long.loc[df_lat_long["distance"] <= max_distance]
            rlog.warn(
                f"Removed {len(problematic_entries)} entries with distance to nearest OSM node larger than {max_distance} meters"
            )
        elif outlier_method == OutlierMethod.WARN:
            rlog.warn(
                f"Found {len(problematic_entries)} entries with distance to nearest OSM node larger than {max_distance} meters"
            )
            for index, row in problematic_entries.iterrows():
                rlog.warn(
                    f"lat: {row['lat']}, lon: {row['lon']}, nearest_osm_node_id: {row['nearest_osm_node_id']}, distance: {row['distance']}"
                )

    return df_lat_long


def list_column_to_osm_nodes(
    osm_nodes_df: pd.DataFrame, df: pd.DataFrame, column: str
) -> pd.DataFrame:
    """
    Assigns each entry in df to a node in osm_nodes_df and lists all the values of column
    for each node.

    Args:
        df: The dataframe to assign to osm_nodes_df. Must contain columns "nearest_osm_node_id" and column.
        osm_nodes_df: The dataframe to assign df to. The index must be the osm node ids.
    """
    grouped: pd.Series = df.groupby("nearest_osm_node_id")[column].agg(
        lambda x: list(set(x))
    )  # type: ignore
    # drop column if it already exists to make this function idempotent
    if column in osm_nodes_df.columns:
        osm_nodes_df = osm_nodes_df.drop(columns=[column])
    osm_nodes_df = osm_nodes_df.merge(
        grouped, left_index=True, right_index=True, how="left"
    )
    osm_nodes_df[column] = osm_nodes_df[column].apply(
        lambda x: x if isinstance(x, list) else []
    )
    # osm_nodes_df = osm_nodes_df.drop(columns=["nearest_osm_node_id"])

    return osm_nodes_df
