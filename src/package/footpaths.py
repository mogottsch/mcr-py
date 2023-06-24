from pyrosm.data import get_data
import shutil
import os
import pyrosm
import tempfile
import geopandas as gpd
from shapely.geometry import MultiPoint
import networkx as nx
import igraph as ig
import osmnx as ox
from tqdm.contrib.concurrent import process_map

from package import storage
from package.logger import Timed, llog


OSM_DIR_PATH = os.path.join(tempfile.gettempdir(), "mcr-py", "osm")


def generate(
    city_id: str,
    osm_path: str,
    stops_path: str,
    avg_walking_speed: float,
    max_walking_duration: int,
):
    osm_path = osm_path if osm_path else get_osm_path_from_city_id(city_id)

    if not os.path.exists(osm_path) and city_id:
        llog.info("Downloading OSM data")
        download_osm(city_id, osm_path)
    else:
        llog.info("Using existing OSM data")

    osm = pyrosm.OSM(osm_path)

    with Timed.info("Reading OSM network"):
        network = osm.get_network(nodes=True, network_type="walking")
        if network is None:
            raise Exception("No walking network found in OSM data.")
        res = network
        nodes: gpd.GeoDataFrame = res[0]  # type: ignore
        edges: gpd.GeoDataFrame = res[1]  # type: ignore

    with Timed.info("Reading stops"):
        stops_df = storage.read_gdf(stops_path)

    with Timed.info("Trimming OSM network to stops"):
        nodes, edges = trim_osm_to_stops(nodes, edges, stops_df)

    with Timed.info("Creating igraph graph"):
        i_graph = create_i_graph(osm, nodes, edges)

    with Timed.info("Creating networkx graph"):
        nx_graph = create_nx_graph(osm, nodes, edges)

    with Timed.info("Adding nearest network node to each stop"):
        stops_df = add_nearest_node_to_stops(stops_df, nx_graph)

    with Timed.info("Finding potential nearby stops for each stop"):
        nearby_stops_map = create_nearby_stops_map(
            stops_df, avg_walking_speed, max_walking_duration
        )

    with Timed.info("Calculating distances between nearby stops"):
        footpaths = create_footpaths(
            i_graph,
            stops_df,
            nearby_stops_map,
            avg_walking_speed,
        )
    return footpaths


def get_osm_path_from_city_id(city_id: str) -> str:
    return os.path.join(OSM_DIR_PATH, f"{city_id}.pbf")


def download_osm(city_id: str, path: str):
    fp = get_data(city_id, update=True)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    shutil.move(fp, path)


def trim_osm_to_stops(
    nodes: gpd.GeoDataFrame, edges: gpd.GeoDataFrame, stops_df: gpd.GeoDataFrame
):
    combined_geometry = MultiPoint(stops_df.geometry.tolist())
    convex_hull = combined_geometry.convex_hull
    zone_of_interest = convex_hull.buffer(0.01)

    n_nodes_before = len(nodes)
    n_edges_before = len(edges)

    nodes = nodes.loc[nodes.geometry.within(zone_of_interest), :]
    edges = edges.loc[edges.u.isin(nodes.id) & edges.v.isin(nodes.id), :]
    llog.info(
        f"{len(nodes)}/{n_nodes_before} ({len(nodes)/n_nodes_before*100:.2f}%) nodes remaining"
    )
    llog.info(
        f"{len(edges)}/{n_edges_before} ({len(edges)/n_edges_before*100:.2f}%) edges remaining"
    )

    return nodes, edges


def create_nx_graph(
    osm: pyrosm.OSM, nodes: gpd.GeoDataFrame, edges: gpd.GeoDataFrame
) -> nx.Graph:
    return osm.to_graph(nodes, edges, graph_type="networkx")  # type: ignore


def create_i_graph(
    osm: pyrosm.OSM, nodes: gpd.GeoDataFrame, edges: gpd.GeoDataFrame
) -> ig.Graph:
    return osm.to_graph(nodes, edges, graph_type="igraph")  # type: ignore


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


def create_footpaths(
    i_graph_arg: ig.Graph,
    stops_df: gpd.GeoDataFrame,
    nearby_stops_map: dict[str, list[str]],
    avg_walking_speed: float,
):
    global i_graph  # will be used during multiprocessing
    i_graph = i_graph_arg

    nearby_stops_with_distance_map = {}

    global node_id_to_g_igraph_node_id_map  # will be used during multiprocessing
    node_id_to_g_igraph_node_id_map = {
        node.attributes()["id"]: node.attributes()["node_id"]
        for node in list(i_graph_arg.vs)
    }

    global nearest_node_dict  # will be used during multiprocessing
    nearest_node_dict = stops_df.set_index("stop_id")["nearest_node"].to_dict()

    stop_list, nearby_stops_list = zip(*nearby_stops_map.items())

    # # pick 10 for testing
    # stop_list = stop_list[:10]
    # nearby_stops_list = nearby_stops_list[:10]

    res = process_map(
        get_distances_nearby_stops,
        stop_list,
        nearby_stops_list,
        chunksize=5,
        max_workers=12,
    )

    for source_stop, nearby_stops_with_distance in zip(stop_list, res):
        nearby_stops_with_distance_map[source_stop] = nearby_stops_with_distance

    nearby_stop_with_walking_time_map = {}
    for (
        source_stop,
        nearby_stops_with_distance,
    ) in nearby_stops_with_distance_map.items():
        nearby_stop_with_walking_time_map[source_stop] = {
            target_stop: int(distance / avg_walking_speed)
            for target_stop, distance in nearby_stops_with_distance.items()
        }

    # clean up globals
    del i_graph
    del node_id_to_g_igraph_node_id_map
    del nearest_node_dict

    return nearby_stop_with_walking_time_map


def get_shortest_path_length_igraph(s_node_id, t_node_id):
    paths = i_graph.get_shortest_paths(
        node_id_to_g_igraph_node_id_map[s_node_id],
        node_id_to_g_igraph_node_id_map[t_node_id],
        weights="length",
        output="epath",
    )
    path = paths[0]
    return sum(i_graph.es[epath]["length"] for epath in path)


def get_distances_nearby_stops(
    source_stop,
    nearby_stops,
):
    return {
        target_stop: get_shortest_path_length_igraph(
            nearest_node_dict[source_stop],
            nearest_node_dict[target_stop],
        )
        for target_stop in nearby_stops
    }
