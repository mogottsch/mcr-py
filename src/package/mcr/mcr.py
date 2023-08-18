from typing import Any, Tuple

import pandas as pd
import geopandas as gpd

import mcr_py
from mcr_py import GraphCache
import pyrosm
from package import storage
from package.logger import Timed
from package.mcr.path import PathManager, PathType
from package.osm import osm, graph
from package.rust.bag import convert_to_intermediate_bags


ACCURACY = 1
ACCURACY_MULTIPLIER = 10 ** (ACCURACY - 1)

AVG_WALKING_SPEED = 1.4  # m/s
AVG_BIKING_SPEED = 4.0  # m/s


def run(
    stops_path: str,
    city_id: str = "",
    osm_path: str = "",
):
    with Timed.info("Reading stops"):
        stops_df = storage.read_gdf(stops_path)

    with Timed.info("Preparing graphs"):
        osm_reader = osm.get_osm_reader_for_city_id_or_osm_path(city_id, osm_path)
        nodes, edges = osm.get_graph_for_city_cropped_to_stops(osm_reader, stops_df)
        nxgraph = graph.create_nx_graph(osm_reader, nodes, edges)

        nodes: pd.DataFrame = nodes[["id"]]  # type: ignore
        edges: pd.DataFrame = edges[["u", "v", "length"]]  # type: ignore

        stops_df = graph.add_nearest_node_to_stops(stops_df, nxgraph)

        nodes = mark_bicycles(nodes)

        graph_components = create_multi_modal_graph(nodes, edges)
        nodes, edges, walking_nodes, walking_edges = (
            graph_components["nodes"],
            graph_components["edges"],
            graph_components["walking_nodes"],
            graph_components["walking_edges"],
        )

        nodes, edges, node_map = reset_node_ids(nodes, edges)
        walking_nodes, walking_edges, walking_node_map = reset_node_ids(
            walking_nodes, walking_edges
        )
        reverse_node_map = get_reverse_map(node_map)
        reverse_walking_node_map = get_reverse_map(walking_node_map)

        edges = add_multi_modal_weights(edges)
        walking_edges = add_single_modal_weights(walking_edges)

        raw_edges = edges[["u", "v", "weights", "hidden_weights"]].to_dict("records")
        raw_walking_edges = walking_edges[["u", "v", "weights"]].to_dict("records")

    bicycle_transfer_nodes_walking_node_ids = walking_nodes[
        walking_nodes["has_bicycle"]
    ].id.values

    with Timed.info("Creating graph cache"):
        gc = GraphCache()
        gc.set_graph(raw_edges)
        walking_gc = GraphCache()
        walking_gc.set_graph(raw_walking_edges)

    with Timed.info("Running Dijkstra step"):
        start_node_id = 295101994
        bags = mcr_py.run_mlc(walking_gc, walking_node_map[f"W{start_node_id}"])

    path_manager = PathManager()
    intermediate_bags = convert_to_intermediate_bags(bags)
    path_manager.extract_all_paths_from_bags(intermediate_bags, PathType.WALKING)

    # translates a node id from the walking graph to the corresponding bicycle
    # node id from the multi-modal graph
    def translate_walking_node_id_to_bicycle_node_id(
        walking_node_id: int,
    ) -> int:
        original_walking_node = reverse_walking_node_map[walking_node_id]
        original_bicycle_node = original_walking_node.replace("W", "B")
        bicycle_node_id = node_map[original_bicycle_node]
        return bicycle_node_id

    # filter bags at bicycle nodes
    bicycle_bags = {
        node_id: bag
        for node_id, bag in intermediate_bags.items()
        if node_id in bicycle_transfer_nodes_walking_node_ids
    }
    # translate node ids
    bicycle_bags = {
        translate_walking_node_id_to_bicycle_node_id(node_id): [
            label.to_mlc_label(translate_walking_node_id_to_bicycle_node_id(node_id))
            for label in labels
        ]
        for node_id, labels in bicycle_bags.items()
    }

    # validation
    for node_id in bicycle_bags.keys():
        gc.validate_node_id(node_id)

    new_bags = mcr_py.run_mlc_with_bags(gc, bicycle_bags)  # type: ignore

    new_intermediate_bags = convert_to_intermediate_bags(new_bags)
    path_manager.extract_all_paths_from_bags(new_intermediate_bags, PathType.CYCLING_WALKING, path_index_offset=1)

    storage.write_any_dict(
        {
            "intermediate_bags": intermediate_bags,
            "path_manager": path_manager,
            "new_intermediate_bags": new_intermediate_bags,
            "node_map": node_map,
            "walking_node_map": walking_node_map,
        },
        "/home/moritz/dev/uni/mcr-py/data/bags.pkl",  # type: ignore
    )


def prefix_id(
    gdf: pd.DataFrame, prefix: str, column: str, save_old=False
) -> pd.DataFrame:
    if save_old:
        gdf[f"{column}_old"] = gdf[column]
    gdf[column] = prefix + gdf[column].astype(str)

    return gdf


def get_graph(
    osm_reader: pyrosm.OSM, stops_df: gpd.GeoDataFrame
) -> Tuple[gpd.GeoDataFrame, gpd.GeoDataFrame]:
    with Timed.info("Getting OSM graph"):
        nodes, edges = osm.get_graph_for_city_cropped_to_stops(osm_reader, stops_df)

    return nodes, edges


def mark_bicycles(nodes: pd.DataFrame) -> pd.DataFrame:
    nodes["has_bicycle"] = False
    nodes.loc[nodes.sample(100).index, "has_bicycle"] = True
    return nodes


def create_multi_modal_graph(
    nodes: pd.DataFrame, edges: pd.DataFrame
) -> dict[str, pd.DataFrame]:
    walking_nodes = nodes.copy()
    bike_nodes = nodes.copy()
    walking_edges = edges.copy()
    bike_edges = edges.copy()

    walking_nodes = prefix_id(walking_nodes, "W", "id", save_old=True)
    bike_nodes = prefix_id(bike_nodes, "B", "id", save_old=True)

    walking_edges = prefix_id(walking_edges, "W", "u")
    walking_edges = prefix_id(walking_edges, "W", "v")
    bike_edges = prefix_id(bike_edges, "B", "u")
    bike_edges = prefix_id(bike_edges, "B", "v")

    transfer_edges = create_transfer_edges(nodes)

    walking_edges = add_travel_time(walking_edges, AVG_WALKING_SPEED)
    bike_edges = add_travel_time(bike_edges, AVG_BIKING_SPEED)

    bike_edges["travel_time_bike"] = bike_edges["travel_time"]

    edges = combine_edges(walking_edges, bike_edges, transfer_edges)
    nodes = pd.concat([walking_nodes, bike_nodes])
    return {
        "nodes": nodes,
        "edges": edges,
        "walking_nodes": walking_nodes,
        "walking_edges": walking_edges,
    }


# create transfer edges from bike to walk at all nodes
def create_transfer_edges(nodes: pd.DataFrame):
    transfer_edges_values: pd.Series = nodes.apply(
        lambda x: ["B" + str(x.id), "W" + str(x.id), 0], axis=1
    )  # type: ignore
    transfer_edges = pd.DataFrame(
        transfer_edges_values.tolist(), columns=["u", "v", "length"]
    )

    return transfer_edges


def add_travel_time(edges: pd.DataFrame, speed: float) -> pd.DataFrame:
    edges["travel_time"] = edges.length / speed

    return edges


def combine_edges(
    walking_edges: pd.DataFrame,
    bike_edges: pd.DataFrame,
    transfer_edges: pd.DataFrame,
) -> pd.DataFrame:
    edges = pd.concat([walking_edges, bike_edges, transfer_edges], ignore_index=True)

    # fill travel_time for transfer edges and
    # travel_time_bike for walking and transfer edges
    edges = edges.fillna(0)

    return edges


def add_multi_modal_weights(edges: pd.DataFrame) -> pd.DataFrame:
    edges["weights"] = (
        "("
        + (edges["travel_time"].round(ACCURACY) * ACCURACY_MULTIPLIER)
        .astype(int)
        .astype(str)
        + ",0)"
    )
    edges["hidden_weights"] = (
        "("
        + (edges["travel_time_bike"].round(ACCURACY) * ACCURACY_MULTIPLIER)
        .astype(int)
        .astype(str)
        + ")"
    )

    return edges


def add_single_modal_weights(edges: pd.DataFrame) -> pd.DataFrame:
    edges["weights"] = (
        "("
        + (edges["travel_time"].round(ACCURACY) * ACCURACY_MULTIPLIER)
        .astype(int)
        .astype(str)
        + ")"
    )
    return edges


def reset_node_ids(
    nodes: pd.DataFrame, edges: pd.DataFrame
) -> Tuple[pd.DataFrame, pd.DataFrame, dict[Any, int]]:
    node_map = {}
    for i, node_id in enumerate(nodes.id.unique()):
        node_map[node_id] = i

    nodes["old_id"] = nodes["id"]
    nodes["id"] = nodes["id"].map(node_map)
    edges["u"] = edges["u"].map(node_map)
    edges["v"] = edges["v"].map(node_map)

    total_na = edges.isna().sum().sum() + nodes.isna().sum().sum()
    if total_na > 0:
        raise ValueError(f"Found {total_na} NaNs in graph")

    return nodes, edges, node_map


def get_reverse_map(d: dict[Any, Any]) -> dict[Any, Any]:
    return {v: k for k, v in d.items()}
