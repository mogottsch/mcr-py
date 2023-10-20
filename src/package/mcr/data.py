from typing import Tuple, TypeVar
import pandas as pd
import geopandas as gpd

from mcr_py import GraphCache
from package import storage
from package.geometa import GeoMeta
from package.logger import Timed, rlog
from package.osm import osm, graph


ACCURACY = 1
ACCURACY_MULTIPLIER = 10 ** (ACCURACY - 1)

AVG_WALKING_SPEED = 1.4  # m/s
AVG_BIKING_SPEED = 4.0  # m/s

N_TOTAL_WEIGHTS = 2  # time, cost
N_TOTAL_HIDDEN_WEIGHTS = 2  # biking time, public transport stops


class OSMData:
    def __init__(
        self,
        geo_meta: GeoMeta,
        city_id: str = "",
        osm_path: str = "",
    ):
        osm_reader = osm.get_osm_reader_for_city_id_or_osm_path(city_id, osm_path)
        (
            osm_nodes,
            osm_edges,
        ) = osm.get_graph_for_city_cropped_to_boundary(osm_reader, geo_meta)
        nxgraph = graph.create_nx_graph(osm_reader, osm_nodes, osm_edges)
        nxgraph, osm_nodes, osm_edges = graph.crop_graph_to_largest_component(
            nxgraph, osm_nodes, osm_edges
        )

        osm_nodes = osm_nodes.set_index("id")
        osm_nodes["id"] = osm_nodes.index

        self.nxgraph = nxgraph
        self.osm_nodes_with_coordinates = osm_nodes.copy()

        osm_nodes: pd.DataFrame = osm_nodes[["id"]]  # type: ignore
        osm_edges: pd.DataFrame = osm_edges[["u", "v", "length"]]  # type: ignore

        self.osm_nodes = osm_nodes
        self.osm_edges = osm_edges


def create_walking_graph(
    osm_nodes: pd.DataFrame, osm_edges: pd.DataFrame
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    walking_nodes = osm_nodes.copy()
    walking_edges = osm_edges.copy()

    walking_edges = add_reverse_edges(walking_edges)

    walking_edges = add_travel_time(walking_edges, AVG_WALKING_SPEED)

    return walking_nodes, walking_edges


def create_multi_modal_graph(
    osm_nodes: pd.DataFrame, osm_edges: pd.DataFrame
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    osm_edges = add_reverse_edges(osm_edges)

    # bike start
    bike_nodes = osm_nodes.copy()
    bike_edges = osm_edges.copy()

    bike_nodes = prefix_id(bike_nodes, "B", "id", save_old=True)
    bike_edges = prefix_id(bike_edges, "B", "u")
    bike_edges = prefix_id(bike_edges, "B", "v")

    bike_edges = add_travel_time(bike_edges, AVG_BIKING_SPEED)
    bike_edges["travel_time_bike"] = bike_edges["travel_time"]
    # bike end

    # walking start
    walking_nodes = osm_nodes.copy()
    walking_edges = osm_edges.copy()

    walking_edges = add_reverse_edges(walking_edges)

    walking_nodes = prefix_id(walking_nodes, "W", "id", save_old=True)
    walking_edges = prefix_id(walking_edges, "W", "u")
    walking_edges = prefix_id(walking_edges, "W", "v")

    walking_edges = add_travel_time(walking_edges, AVG_WALKING_SPEED)
    # walking end

    transfer_edges = create_transfer_edges(osm_nodes)

    multi_modal_edges = combine_edges(walking_edges, bike_edges, transfer_edges)
    multi_modal_nodes = pd.concat([walking_nodes, bike_nodes])
    return multi_modal_nodes, multi_modal_edges


def add_reverse_edges(edges: pd.DataFrame) -> pd.DataFrame:
    reverse_edges = edges.copy()
    reverse_edges = reverse_edges.rename(columns={"u": "v", "v": "u"})
    return pd.concat([edges, reverse_edges])


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


def reset_node_ids(
    nodes: pd.DataFrame, edges: pd.DataFrame
) -> Tuple[pd.DataFrame, pd.DataFrame, dict[str, int]]:
    node_to_resetted_map: dict[str, int] = {}
    for i, node_id in enumerate(nodes.id.unique()):
        node_to_resetted_map[node_id] = i

    nodes["old_id"] = nodes["id"]
    nodes["id"] = nodes["id"].map(node_to_resetted_map)  # type: ignore
    edges["u"] = edges["u"].map(node_to_resetted_map)  # type: ignore
    edges["v"] = edges["v"].map(node_to_resetted_map)  # type: ignore

    edges_na = edges[["u", "v"]].isna().sum().sum()
    nodes_na = nodes["id"].isna().sum()
    total_na = edges_na + nodes_na
    if total_na > 0:
        raise ValueError(
            f"Found {total_na} NaNs in graph (edges: {edges_na}, nodes: {nodes_na})"
        )

    return nodes, edges, node_to_resetted_map


A = TypeVar("A")
B = TypeVar("B")


def get_reverse_map(d: dict[A, B]) -> dict[B, A]:
    return {v: k for k, v in d.items()}


def prefix_id(
    gdf: pd.DataFrame, prefix: str, column: str, save_old=False
) -> pd.DataFrame:
    if save_old:
        gdf[f"{column}_old"] = gdf[column]
    gdf[column] = prefix + gdf[column].astype(str)

    return gdf


def create_transfer_edges(nodes: pd.DataFrame):
    transfer_edges_values: pd.Series = nodes.apply(
        lambda x: ["B" + str(x.id), "W" + str(x.id), 0], axis=1
    )  # type: ignore
    transfer_edges = pd.DataFrame(
        transfer_edges_values.tolist(), columns=["u", "v", "length"]
    )

    return transfer_edges


def add_weights(edges: pd.DataFrame, columns: list[str], hidden=False) -> pd.DataFrame:
    col_name = "hidden_weights" if hidden else "weights"
    n_padding = N_TOTAL_HIDDEN_WEIGHTS if hidden else N_TOTAL_WEIGHTS

    mid_seperator = "," if len(columns) > 0 and n_padding > 0 else ""

    edges[col_name] = (
        "("
        + (edges[columns].round(ACCURACY) * ACCURACY_MULTIPLIER)
        .astype(int)
        .astype(str)
        .apply(lambda x: ",".join(x), axis=1)
        + mid_seperator
        + ",".join(["0"] * (n_padding - len(columns)))
        + ")"
    )

    return edges
