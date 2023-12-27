import multiprocessing

import pyrosm
import geopandas as gpd
import igraph as ig
from tqdm.contrib.concurrent import process_map
from package import key

from package.logger import Timed


# retrieves a dictionary, where the keys are source and the values are a list of targets
# returns a dictionary, where the keys are source and the values are a dictionary of targets and distances
def query_multiple_one_to_many(
    source_target_nodes_map: dict[int, list[int]],
    osm_reader: pyrosm.OSM,
    nodes: gpd.GeoDataFrame,
    edges: gpd.GeoDataFrame,
) -> dict[int, dict[int, float]]:
    global i_graph  # will be used during multiprocessing
    # TODO: we could probably use a class to avoid this global variable
    with Timed.info("Creating igraph graph"):
        i_graph = create_i_graph(osm_reader, nodes, edges)

    (
        node_id_to_g_igraph_node_id_map,
        igraph_node_id_to_node_id_map,
    ) = get_conversion_maps(i_graph)

    # convert to igraph node ids
    source_target_nodes_map_igraph: dict[int, list[int]] = {
        node_id_to_g_igraph_node_id_map[node_id]: [
            node_id_to_g_igraph_node_id_map[node_id] for node_id in nearby_nodes
        ]
        for node_id, nearby_nodes in source_target_nodes_map.items()
    }

    source_nodes, target_nodes_matrix = zip(*source_target_nodes_map_igraph.items())

    res = process_map(
        get_shortest_path_one_to_many,
        source_nodes,
        target_nodes_matrix,
        chunksize=5,
        max_workers=key.DEFAULT_N_PROCESSES,
    )

    source_target_nodes_distance_map: dict[int, dict[int, float]] = {}
    for source_node, nearby_nodes_with_distance in zip(source_nodes, res):
        source_node = igraph_node_id_to_node_id_map[source_node]  # type: ignore
        source_target_nodes_distance_map[source_node] = {
            igraph_node_id_to_node_id_map[target_node]: distance
            for target_node, distance in nearby_nodes_with_distance.items()
        }

    del i_graph

    return source_target_nodes_distance_map


def get_conversion_maps(
    igraph: ig.Graph,
) -> tuple[dict[int, int], dict[int, int]]:
    node_id_to_g_igraph_node_id_map = {
        node.attributes()["id"]: node.attributes()["node_id"]
        for node in list(igraph.vs)
    }
    igraph_node_id_to_node_id_map = {
        node.attributes()["node_id"]: node.attributes()["id"]
        for node in list(igraph.vs)
    }
    return node_id_to_g_igraph_node_id_map, igraph_node_id_to_node_id_map


def create_i_graph(
    osm: pyrosm.OSM, nodes: gpd.GeoDataFrame, edges: gpd.GeoDataFrame
) -> ig.Graph:
    return osm.to_graph(nodes, edges, graph_type="igraph", network_type="walking")  # type: ignore


def get_shortest_path(source_node: int, target_node: int) -> int:
    paths = i_graph.get_shortest_paths(
        source_node,
        target_node,
        weights="length",
        output="epath",
    )
    path = paths[0]
    return sum(i_graph.es[epath]["length"] for epath in path)


def get_shortest_path_one_to_many(
    source_node: int,
    target_nodes: list[int],
):
    return {
        target_stop: get_shortest_path(
            source_node,
            target_stop,
        )
        for target_stop in target_nodes
    }
