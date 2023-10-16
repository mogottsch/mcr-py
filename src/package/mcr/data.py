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


class MCRGeoData:
    def __init__(
        self,
        stops_path: str,
        structs_path: str,
        geo_meta: GeoMeta,
        city_id: str = "",
        osm_path: str = "",
        bicycle_location_path: str = "",
        logger=rlog,
    ):
        self.structs_path = structs_path
        self.stops_path = stops_path
        self.city_id = city_id
        self.osm_path = osm_path
        self.logger = logger
        self.geo_meta = geo_meta

        self.structs_dict = storage.read_any_dict(structs_path)
        with Timed.info("Reading stops and geo meta"):
            self.stops_df = storage.read_gdf(stops_path)

        with Timed.info("Preparing graphs"):
            osm_reader = osm.get_osm_reader_for_city_id_or_osm_path(city_id, osm_path)
            (
                osm_nodes,
                osm_edges,
            ) = osm.get_graph_for_city_cropped_to_boundary(osm_reader, self.geo_meta)
            nxgraph = graph.create_nx_graph(osm_reader, osm_nodes, osm_edges)
            nxgraph, osm_nodes, osm_edges = graph.crop_graph_to_largest_component(
                nxgraph, osm_nodes, osm_edges
            )

            osm_nodes = osm_nodes.set_index("id")
            osm_nodes["id"] = osm_nodes.index

            self.bicycle_locations = None
            if bicycle_location_path != "":
                self.bicycle_locations = storage.read_df(bicycle_location_path)
                self.bicycle_locations = gpd.GeoDataFrame(
                    self.bicycle_locations,
                    geometry=gpd.points_from_xy(
                        self.bicycle_locations.lon,
                        self.bicycle_locations.lat,
                    ),
                )
                self.bicycle_locations = self.geo_meta.crop_gdf(self.bicycle_locations)
                self.bicycle_locations = osm.add_nearest_osm_node_id(
                    self.bicycle_locations, osm_nodes
                )
            else:
                self.logger.warn(
                    "No bicycle locations provided - will use random locations"
                )

            osm_nodes: pd.DataFrame = osm_nodes[["id"]]  # type: ignore
            osm_edges: pd.DataFrame = osm_edges[["u", "v", "length"]]  # type: ignore

            stops_df = graph.add_nearest_node_to_stops(self.stops_df, nxgraph)

            stops_df["stop_id"] = stops_df["stop_id"].astype(int)
            self.stop_to_osm_node_map: dict[int, int] = stops_df.set_index("stop_id")[
                "nearest_node"
            ].to_dict()
            self.osm_node_to_stop_map: dict[int, int] = {
                v: k for k, v in self.stop_to_osm_node_map.items()
            }

            if self.bicycle_locations is not None:
                osm_nodes = mark_bicycles(osm_nodes, self.bicycle_locations)
            else:
                osm_nodes = mark_bicycles_random(osm_nodes, 100)

            self.bicycle_transfer_nodes_walking_node_ids = osm_nodes[
                osm_nodes["has_bicycle"]
            ].id.values

            graph_components = create_multi_modal_graph(osm_nodes, osm_edges)
            multi_modal_nodes, multi_modal_edges, walking_nodes, walking_edges = (
                graph_components["multi_modal_nodes"],
                graph_components["multi_modal_edges"],
                graph_components["walking_nodes"],
                graph_components["walking_edges"],
            )

            (
                multi_modal_nodes,
                multi_modal_edges,
                self.multi_modal_node_to_resetted_map,
            ) = reset_node_ids(multi_modal_nodes, multi_modal_edges)
            (
                walking_nodes,
                walking_edges,
                self.walking_node_to_resetted_map,
            ) = reset_node_ids(walking_nodes, walking_edges)

            self.resetted_to_multi_modal_node_map = get_reverse_map(
                self.multi_modal_node_to_resetted_map
            )
            self.resetted_to_walking_node_map = get_reverse_map(
                self.walking_node_to_resetted_map
            )

            multi_modal_edges = add_weights(multi_modal_edges, ["travel_time"])
            multi_modal_edges = add_weights(
                multi_modal_edges, ["travel_time_bike"], hidden=True
            )
            self.walking_edges = add_weights(walking_edges, ["travel_time"])
            self.walking_edges = add_weights(walking_edges, [], hidden=True)

            raw_edges = multi_modal_edges[
                ["u", "v", "weights", "hidden_weights"]
            ].to_dict(
                "records"  # type: ignore
            )
            raw_walking_edges = walking_edges[
                ["u", "v", "weights", "hidden_weights"]
            ].to_dict(
                "records"  # type: ignore
            )
            self.osm_nodes = osm_nodes

        with Timed.info("Creating graph cache"):
            self.mm_graph_cache = GraphCache()
            self.mm_graph_cache.set_graph(raw_edges)
            self.walking_graph_cache = GraphCache()
            self.walking_graph_cache.set_graph(raw_walking_edges)

    def add_pois_to_mm_graph(self, pois: pd.DataFrame) -> None:
        """
        Adds POIs to the multi modal graph cache.

        Args:
            pois: A dataframe containing POIs. Must have the columns "nearest_osm_node_id" and "type".
        """
        self.osm_nodes = osm.list_column_to_osm_nodes(self.osm_nodes, pois, "type")
        self.type_map: dict[str, int] = {}
        for t in pois["type"].unique():
            self.type_map[t] = len(self.type_map)

        self.osm_nodes["type_internal"] = self.osm_nodes["type"].map(
            lambda x: list(map(self.type_map.get, x))
        )
        self.osm_nodes["mm_walking_node_id"] = "W" + self.osm_nodes["id"].astype(str)
        self.osm_nodes["resetted_mm_walking_node_id"] = self.osm_nodes[
            "mm_walking_node_id"
        ].map(
            self.multi_modal_node_to_resetted_map  # type: ignore
        )

        resetted_mm_walking_node_id_to_type_map = (
            self.osm_nodes[["resetted_mm_walking_node_id", "type_internal"]].set_index(
                "resetted_mm_walking_node_id"
            )["type_internal"]
        ).to_dict()

        self.mm_graph_cache.set_node_weights(resetted_mm_walking_node_id_to_type_map)


def mark_bicycles(nodes: pd.DataFrame, bicycle_locations: pd.DataFrame) -> pd.DataFrame:
    nodes["has_bicycle"] = False

    nodes.loc[bicycle_locations["nearest_osm_node_id"], "has_bicycle"] = True

    return nodes


def mark_bicycles_random(nodes: pd.DataFrame, n: int) -> pd.DataFrame:
    nodes["has_bicycle"] = False

    nodes.loc[nodes.sample(n).index, "has_bicycle"] = True

    return nodes


def create_multi_modal_graph(
    multi_modal_nodes: pd.DataFrame, multi_modaledges: pd.DataFrame
) -> dict[str, pd.DataFrame]:
    multi_modaledges = add_reverse_edges(multi_modaledges)

    walking_nodes = multi_modal_nodes.copy()
    bike_nodes = multi_modal_nodes.copy()
    walking_edges = multi_modaledges.copy()
    bike_edges = multi_modaledges.copy()

    walking_nodes = prefix_id(walking_nodes, "W", "id", save_old=True)
    bike_nodes = prefix_id(bike_nodes, "B", "id", save_old=True)

    walking_edges = prefix_id(walking_edges, "W", "u")
    walking_edges = prefix_id(walking_edges, "W", "v")
    bike_edges = prefix_id(bike_edges, "B", "u")
    bike_edges = prefix_id(bike_edges, "B", "v")

    transfer_edges = create_transfer_edges(multi_modal_nodes)

    walking_edges = add_travel_time(walking_edges, AVG_WALKING_SPEED)
    bike_edges = add_travel_time(bike_edges, AVG_BIKING_SPEED)

    bike_edges["travel_time_bike"] = bike_edges["travel_time"]

    multi_modaledges = combine_edges(walking_edges, bike_edges, transfer_edges)
    multi_modal_nodes = pd.concat([walking_nodes, bike_nodes])
    return {
        "multi_modal_nodes": multi_modal_nodes,
        "multi_modal_edges": multi_modaledges,
        "walking_nodes": walking_nodes,
        "walking_edges": walking_edges,
    }


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

    total_na = edges.isna().sum().sum() + nodes.isna().sum().sum()
    if total_na > 0:
        raise ValueError(f"Found {total_na} NaNs in graph")

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
