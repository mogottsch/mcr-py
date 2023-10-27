from enum import Enum
from typing import Tuple, TypeVar
import pandas as pd
import networkx as nx

from package.geometa import GeoMeta
from package.logger import rlog
from package.osm import osm, graph


ACCURACY = 1
ACCURACY_MULTIPLIER = 10 ** (ACCURACY - 1)

AVG_WALKING_SPEED = 1.4  # m/s
AVG_BIKING_SPEED = 4.0  # m/s
AVG_CAR_SPEED = 11.0  # m/s

N_TOTAL_WEIGHTS = 2  # time, cost
N_TOTAL_HIDDEN_WEIGHTS = 2  # biking time, public transport stops


class NetworkType(Enum):
    WALKING = "walking"
    CYCLING = "cycling"
    DRIVING = "driving"


class OSMData:
    def __init__(
        self,
        geo_meta: GeoMeta,
        city_id: str = "",
        osm_path: str = "",
        additional_network_types: list[NetworkType] = [],
    ):
        self.geo_meta = geo_meta
        self.city_id = city_id
        self.osm_path = osm_path

        self.osm_nodes, self.osm_edges, self.nxgraph = self.read_network("walking")

        self.additional_networks: dict[
            NetworkType, tuple[pd.DataFrame, pd.DataFrame, nx.Graph]
        ] = {}

        for network_type in additional_network_types:
            (
                osm_nodes,
                osm_edges,
                nxgraph,
            ) = self.read_network(network_type.value)
            self.additional_networks[network_type] = (
                osm_nodes,
                osm_edges,
                nxgraph,
            )

    def read_network(
        self,
        network_type: str,
    ) -> tuple[pd.DataFrame, pd.DataFrame, nx.Graph]:
        osm_reader = osm.get_osm_reader_for_city_id_or_osm_path(
            self.city_id, self.osm_path
        )
        (
            osm_nodes,
            osm_edges,
        ) = osm.get_graph_for_city_cropped_to_boundary(
            osm_reader, self.geo_meta, network_type
        )
        nxgraph = graph.create_nx_graph(osm_reader, osm_nodes, osm_edges, network_type)
        nxgraph, osm_nodes, osm_edges = graph.crop_graph_to_largest_component(
            nxgraph, osm_nodes, osm_edges
        )

        osm_nodes = osm_nodes.set_index("id")
        osm_nodes["id"] = osm_nodes.index

        osm_edges: pd.DataFrame = osm_edges[["u", "v", "length"]]  # type: ignore

        return osm_nodes, osm_edges, nxgraph


def create_walking_graph(
    osm_nodes: pd.DataFrame, osm_edges: pd.DataFrame
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    walking_nodes = osm_nodes.copy()
    walking_edges = osm_edges.copy()

    walking_edges = add_reverse_edges(walking_edges)

    walking_edges = add_travel_time(walking_edges, AVG_WALKING_SPEED)

    return walking_nodes, walking_edges


DRIVING_PREFIX = "D"
WALKING_PREFIX = "W"


def create_multi_modal_graph(
    walking_osm_nodes: pd.DataFrame,
    walking_osm_edges: pd.DataFrame,
    driving_osm_nodes: pd.DataFrame,
    driving_osm_edges: pd.DataFrame,
    avg_driving_speed: float,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    walking_osm_edges = add_reverse_edges(walking_osm_edges)
    driving_osm_edges = add_reverse_edges(driving_osm_edges)

    # bike start
    driving_osm_nodes = driving_osm_nodes.copy()
    driving_osm_nodes = driving_osm_nodes.copy()

    driving_osm_nodes = prefix_id(
        driving_osm_nodes, DRIVING_PREFIX, "id", save_old=True
    )
    driving_osm_edges = prefix_id(driving_osm_edges, DRIVING_PREFIX, "u")
    driving_osm_edges = prefix_id(driving_osm_edges, DRIVING_PREFIX, "v")

    driving_osm_edges = add_travel_time(driving_osm_edges, avg_driving_speed)
    driving_osm_edges[TRAVEL_TIME_DRIVING_COLUMN] = driving_osm_edges[
        TRAVEL_TIME_COLUMN
    ]
    # bike end

    # walking start
    walking_osm_nodes = walking_osm_nodes.copy()
    walking_osm_edges = walking_osm_edges.copy()

    walking_osm_edges = add_reverse_edges(walking_osm_edges)

    walking_osm_nodes = prefix_id(
        walking_osm_nodes, WALKING_PREFIX, "id", save_old=True
    )
    walking_osm_edges = prefix_id(walking_osm_edges, WALKING_PREFIX, "u")
    walking_osm_edges = prefix_id(walking_osm_edges, WALKING_PREFIX, "v")

    walking_osm_edges = add_travel_time(walking_osm_edges, AVG_WALKING_SPEED)
    # walking end

    transfer_edges = create_transfer_edges(walking_osm_nodes, driving_osm_nodes)

    multi_modal_edges = combine_edges(
        walking_osm_edges, driving_osm_edges, transfer_edges
    )
    multi_modal_nodes = pd.concat([walking_osm_nodes, driving_osm_nodes])
    return multi_modal_nodes, multi_modal_edges


def add_reverse_edges(edges: pd.DataFrame) -> pd.DataFrame:
    reverse_edges = edges.copy()
    reverse_edges = reverse_edges.rename(columns={"u": "v", "v": "u"})
    return pd.concat([edges, reverse_edges])


TRAVEL_TIME_COLUMN = "travel_time"
TRAVEL_TIME_DRIVING_COLUMN = "travel_time_driving"


def add_travel_time(edges: pd.DataFrame, speed: float) -> pd.DataFrame:
    edges[TRAVEL_TIME_COLUMN] = edges.length / speed

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


def create_transfer_edges(walking_nodes: pd.DataFrame, driving_nodes: pd.DataFrame):
    intersection_node_ids = walking_nodes.index.intersection(
        driving_nodes.index
    ).to_series()  # type: ignore
    rlog.debug(f"Found {len(intersection_node_ids)} intersection nodes")

    transfer_edges_values: pd.Series = intersection_node_ids.apply(
        lambda x: ["D" + str(x), "W" + str(x), 0]
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


def to_mlc_edges(edges: pd.DataFrame) -> list[dict]:
    return edges[["u", "v", "weights", "hidden_weights"]].to_dict("records")  # type: ignore
