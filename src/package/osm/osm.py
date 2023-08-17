import os
import shutil
from typing import Tuple
from typing_extensions import Annotated

import pyrosm
import geopandas as gpd
from shapely.geometry import MultiPoint
from pyrosm.data import get_data
from package import storage, cache
from package.osm import osm, key as osm_key
from package.logger import Timed, rlog


from package.logger import rlog
from package import console, key, storage

OSM_DIR_PATH = storage.get_tmp_path(key.TMP_DIR_NAME, key.TMP_OSM_DIR_NAME)


def list_available(selector: str):
    root_name = "Available OSM data"
    if selector != "":
        root_name += f" for {selector}"
    console.print_tree_from_any(get_available(selector), root_name=root_name)


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


def get_graph_for_city_cropped_to_stops(
    osm_reader: pyrosm.OSM,
    stops_df: gpd.GeoDataFrame,
) -> Tuple[gpd.GeoDataFrame, gpd.GeoDataFrame]:
    hash = cache.combine_hashes(
        [
            cache.hash_str(osm_reader.filepath),
            cache.hash_gdf(stops_df),
        ]
    )

    rlog.info(f"Hash for OSM network: {hash}")

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

    with Timed.info("Cropping OSM network to stops"):
        nodes, edges = osm.crop_to_stops(nodes, edges, stops_df)

    with Timed.info("Caching OSM network"):
        cache.cache_gdf(nodes, hash, osm_key.NODES_FILE_IDENTIFIER)
        cache.cache_gdf(edges, hash, osm_key.EDGES_FILE_IDENTIFIER)

    return nodes, edges


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


def get_osm_path_from_city_id(city_id: str) -> str:
    return os.path.join(OSM_DIR_PATH, f"{city_id}.pbf")


def download_city(city_id: str, path: str):
    fp = get_data(city_id, update=True)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    shutil.move(fp, path)
