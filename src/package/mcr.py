import os
from typing import Any, Tuple

import pandas as pd
import geopandas as gpd

import mcr_py
from pyrosm.data import os
from package import storage
from package.logger import Timed, rlog
from package.osm import osm


ACCURACY = 4
ACCURACY_MULTIPLIER = 10 ** (ACCURACY - 1)

AVG_WALKING_SPEED = 1.4  # m/s
AVG_BIKING_SPEED = 4.0  # m/s


def run(
    stops_path: str,
    city_id: str = "",
    osm_path: str = "",
):
    nodes, edges = get_graph(city_id, stops_path, osm_path)

    nodes: pd.DataFrame = nodes[["id", "has_bicycle"]]  # type: ignore
    edges: pd.DataFrame = edges[["u", "v", "length"]]  # type: ignore

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

    walking_edges = add_single_modal_weights(walking_edges)

    raw_edges = walking_edges[["u", "v", "weights"]].to_dict("records")
    with Timed.info("Running MLC"):
        bags = mcr_py.run_mlc(raw_edges, 0)
    storage.write_any_dict(bags, "/home/moritz/dev/uni/mcr-py/data/bags.pkl")


def prefix_id(gdf: pd.DataFrame, prefix: str) -> pd.DataFrame:
    gdf["old_id"] = gdf["id"]
    gdf["id"] = prefix + gdf["id"].astype(str)

    return gdf


def get_graph(
    city_id: str, stops_path: str, osm_path: str
) -> Tuple[gpd.GeoDataFrame, gp.GeoDataFrame]:
    osm_path = osm.get_osm_path_from_city_id(city_id)

    with Timed.info("Reading stops"):
        stops_df = storage.read_gdf(stops_path)

    if not os.path.exists(osm_path) and city_id:
        with Timed.info("Downloading OSM data"):
            osm.download_city(city_id, osm_path)
    else:
        rlog.info("Using existing OSM data")

    osm_reader = osm.new_osm_reader(osm_path)

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

    walking_nodes = prefix_id(walking_nodes, "W")
    bike_nodes = prefix_id(bike_nodes, "B")

    walking_edges = prefix_id(walking_edges, "W")
    bike_edges = prefix_id(bike_edges, "B")

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
        lambda x: ["B" + str(x.old_id), "W" + str(x.old_id), 0], axis=1
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
        + ";0)"
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
