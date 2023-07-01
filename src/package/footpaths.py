import shutil
import os
from enum import Enum

from pyrosm.data import get_data
import pyrosm
import geopandas as gpd
import networkx as nx
import osmnx as ox
from rich.pretty import pprint

from package import key, storage, osm
from package.logger import Timed, llog
from package.graph import igraph, fast_path


OSM_DIR_PATH = storage.get_tmp_path(key.TMP_DIR_NAME, key.TMP_OSM_DIR_NAME)


class GenerationMethod(Enum):
    IGRAPH = "igraph"
    FAST_PATH = "fast_path"

    @classmethod
    def from_str(cls, method: str) -> "GenerationMethod":
        if method.upper() not in cls.all():
            raise ValueError(f"Unknown generation method: {method}")
        return cls[method.upper()]

    @classmethod
    def all(cls) -> list[str]:
        return [method.name for method in cls]


def generate(
    city_id: str,
    osm_path: str,
    stops_path: str,
    avg_walking_speed: float,
    max_walking_duration: int,
    method: GenerationMethod = GenerationMethod.IGRAPH,
) -> dict[str, dict[str, int]]:
    osm_path = osm_path if osm_path else get_osm_path_from_city_id(city_id)

    if not os.path.exists(osm_path) and city_id:
        llog.info("Downloading OSM data")
        download_osm(city_id, osm_path)
    else:
        llog.info("Using existing OSM data")

    osm_reader = osm.new_osm_reader(osm_path)

    with Timed.info("Reading OSM network"):
        # TODO: optimally we should first crop the data and then read the network
        nodes, edges = osm.read_network(osm_reader)

    with Timed.info("Reading stops"):
        stops_df = storage.read_gdf(stops_path)

    with Timed.info("Cropping OSM network to stops"):
        nodes, edges = osm.crop_to_stops(nodes, edges, stops_df)

    with Timed.info("Creating networkx graph"):
        nx_graph = create_nx_graph(osm_reader, nodes, edges)

    with Timed.info("Adding nearest network node to each stop"):
        stops_df = add_nearest_node_to_stops(stops_df, nx_graph)

    with Timed.info("Finding potential nearby stops for each stop"):
        nearby_stops_map = create_nearby_stops_map(
            stops_df, avg_walking_speed, max_walking_duration
        )

    stop_to_node_map: dict[str, int] = stops_df.set_index("stop_id")[
        "nearest_node"
    ].to_dict()
    node_to_stop_map: dict[int, str] = stops_df.set_index("nearest_node")[
        "stop_id"
    ].to_dict()

    # this map contains the one-to-many queries that have to be solved on the graph
    source_targets_map: dict[int, list[int]] = {
        stop_to_node_map[stop_id]: [
            stop_to_node_map[stop_id] for stop_id in nearby_stops
        ]
        for stop_id, nearby_stops in nearby_stops_map.items()
    }

    with Timed.info(f"Calculating distances between nearby stops using {method.name}"):
        if method == GenerationMethod.IGRAPH:
            source_targets_distance_map = igraph.query_multiple_one_to_many(
                source_targets_map, osm_reader, nodes, edges
            )
        elif method == GenerationMethod.FAST_PATH:
            source_targets_distance_map = fast_path.query_multiple_one_to_many(
                source_targets_map, edges
            )

    # pprint(node_to_stop_map)
    footpaths: dict[str, dict[str, int]] = {}
    for source_node, targets_distance_map in source_targets_distance_map.items():
        stop_id = node_to_stop_map[source_node]
        footpaths[stop_id] = {  # type: ignore
            node_to_stop_map[target_node]: int(distance / avg_walking_speed)
            for target_node, distance in targets_distance_map.items()
        }

    return footpaths


def get_osm_path_from_city_id(city_id: str) -> str:
    return os.path.join(OSM_DIR_PATH, f"{city_id}.pbf")


def download_osm(city_id: str, path: str):
    fp = get_data(city_id, update=True)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    shutil.move(fp, path)


def create_nx_graph(
    osm: pyrosm.OSM, nodes: gpd.GeoDataFrame, edges: gpd.GeoDataFrame
) -> nx.Graph:
    return osm.to_graph(nodes, edges, graph_type="networkx")  # type: ignore


def add_nearest_node_to_stops(
    stops_df: gpd.GeoDataFrame, nx_graph: nx.Graph
) -> gpd.GeoDataFrame:
    nodes, dists = ox.nearest_nodes(
        nx_graph,
        stops_df.stop_lon.astype(float),  # TODO: improve this
        stops_df.stop_lat.astype(float),
        return_dist=True,
    )  # type: ignore
    stops_df["nearest_node"] = nodes
    stops_df["nearest_node_dist"] = dists
    return stops_df


def create_nearby_stops_map(
    stops_df: gpd.GeoDataFrame,
    avg_walking_speed: float,
    max_walking_duration: int,
) -> dict[str, list[str]]:
    # crs for beeline distance
    stops_df = stops_df.copy().set_crs("EPSG:4326").to_crs("EPSG:32634")  # type: ignore

    max_walking_distance = avg_walking_speed * max_walking_duration

    nearby_stops_map: dict[str, list[str]] = {}
    for _, row in stops_df.iterrows():
        nearby_stops = stops_df.loc[
            stops_df.geometry.distance(row.geometry) < max_walking_distance
        ].stop_id.tolist()

        # remove self
        nearby_stops = [stop_id for stop_id in nearby_stops if stop_id != row.stop_id]
        nearby_stops_map[row.stop_id] = nearby_stops

    return nearby_stops_map
