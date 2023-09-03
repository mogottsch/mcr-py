from copy import deepcopy
from typing import Any, Optional, Tuple, TypeVar

import pandas as pd
import geopandas as gpd

import mcr_py
from mcr_py import PyLabel
from mcr_py import GraphCache
import pyrosm
from package import storage, strtime
from package.logger import Timed, rlog
from package.mcr.label import (
    IntermediateLabel,
    McRAPTORLabel,
    McRAPTORLabelWithPath,
    merge_intermediate_bags,
)
from package.mcr.path import PathManager, PathType
from package.osm import osm, graph
from package.raptor.mcraptor_single import McRaptorSingle
from package.raptor.bag import Bag
from package.mcr.bag import (
    IntermediateBags,
    convert_mc_raptor_bags_to_intermediate_bags,
    convert_mlc_bags_to_intermediate_bags,
)

McRAPTORInputBags = dict[str, list[IntermediateLabel]]


ACCURACY = 1
ACCURACY_MULTIPLIER = 10 ** (ACCURACY - 1)

AVG_WALKING_SPEED = 1.4  # m/s
AVG_BIKING_SPEED = 4.0  # m/s


class MCR:
    def __init__(
        self,
        stops_path: str,
        structs_path: str,
        city_id: str = "",
        osm_path: str = "",
        disable_paths: bool = False,
    ):
        self.structs_path = structs_path
        self.stops_path = stops_path
        self.city_id = city_id
        self.osm_path = osm_path

        self.disable_paths = disable_paths
        self.path_manager: Optional[PathManager] = None
        if not disable_paths:
            self.path_manager = PathManager()

        self.structs_dict = storage.read_any_dict(structs_path)
        with Timed.info("Reading stops"):
            self.stops_df = storage.read_gdf(stops_path)

        with Timed.info("Preparing graphs"):
            osm_reader = osm.get_osm_reader_for_city_id_or_osm_path(city_id, osm_path)
            (
                osm_nodes,
                osm_edges,
            ) = osm.get_graph_for_city_cropped_to_stops(osm_reader, self.stops_df)
            nxgraph = graph.create_nx_graph(osm_reader, osm_nodes, osm_edges)

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

            osm_nodes = mark_bicycles(osm_nodes)
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

            multi_modal_edges = add_multi_modal_weights(multi_modal_edges)
            walking_edges = add_single_modal_weights(walking_edges)

            raw_edges = multi_modal_edges[
                ["u", "v", "weights", "hidden_weights"]
            ].to_dict(
                "records"  # type: ignore
            )
            raw_walking_edges = walking_edges[["u", "v", "weights"]].to_dict("records")  # type: ignore

        with Timed.info("Creating graph cache"):
            self.graph_cache = GraphCache()
            self.graph_cache.set_graph(raw_edges)
            self.walking_graph_cache = GraphCache()
            self.walking_graph_cache.set_graph(raw_walking_edges)

    def run(
        self, start_node_id: int, start_time: str, max_transfers: int, output_path: str
    ):
        start_time_in_seconds = strtime.str_time_to_seconds(start_time)

        bags_i: dict[int, IntermediateBags] = {}

        with Timed.info("Running Dijkstra step"):
            walking_result_bags = mcr_py.run_mlc_with_node_and_time(
                self.walking_graph_cache,
                self.walking_node_to_resetted_map[f"W{start_node_id}"],
                start_time_in_seconds,
                disable_paths=self.disable_paths,
            )

        with Timed.info("Extracting dijkstra step bags"):
            walking_result_bags = self.convert_walking_bags(
                walking_result_bags, add_zero_weight_to_values=True
            )
            bags_i[0] = walking_result_bags
            # bags_i[0] = deepcopy(walking_result_bags)

            n_osm_nodes = len(walking_result_bags)
            rlog.debug(f"Found {n_osm_nodes} osm nodes in walking step")

        for i in range(1, max_transfers + 1):
            rlog.info(f"Running iteration {i}")
            offset = i * 2 - 1

            with Timed.info("Preparing input for bicycle step"):
                bicycle_bags = self.prepare_bicycle_step_input(walking_result_bags)
                assert len(bicycle_bags) > 0

            with Timed.info("Running bicycle step"):
                bicycle_result_bags = mcr_py.run_mlc_with_bags(
                    self.graph_cache,
                    bicycle_bags,
                    update_label_func="next_bike_tariff",
                    disable_paths=self.disable_paths,
                )

            with Timed.info("Extracting bicycle step bags"):
                bicycle_result_bags = self.convert_bicycle_bags(
                    bicycle_result_bags, path_index_offset=offset
                )
                assert len(bicycle_result_bags) == n_osm_nodes

            with Timed.info("Preparing input for MCRAPTOR step"):
                mc_raptor_bags = self.prepare_mcraptor_step_input(walking_result_bags)

            with Timed.info("Running MCRAPTOR step"):
                mc_raptor = McRaptorSingle(
                    self.structs_dict,
                    default_transfer_time=60,
                    label_class=McRAPTORLabel
                    if self.disable_paths
                    else McRAPTORLabelWithPath,
                )
                mc_raptor_result_bags = mc_raptor.run(mc_raptor_bags)  # type: ignore

            with Timed.info("Extracting MCRAPTOR step bags"):
                mc_raptor_result_bags = self.convert_mc_raptor_bags(
                    mc_raptor_result_bags, path_index_offset=offset
                )
                if len(mc_raptor_result_bags) == 0:
                    rlog.warn("No MCRAPTOR bags found - the area might be too small")

            with Timed.info("Merging bags"):
                combined_bags = self.merge_bags(
                    bicycle_result_bags,
                    mc_raptor_result_bags,  # i am really unsure whether the input here has the same node ids type
                )
                assert len(combined_bags) == n_osm_nodes

            with Timed.info("Preparing input for walking step"):
                walking_bags = self.prepare_walking_step_input(combined_bags)

            with Timed.info("Running walking step"):
                walking_result_bags = mcr_py.run_mlc_with_bags(
                    self.walking_graph_cache,
                    walking_bags,
                    disable_paths=self.disable_paths,
                )
            with Timed.info("Extracting walking step bags"):
                walking_result_bags = self.convert_walking_bags(
                    walking_result_bags, path_index_offset=offset + 1
                )
                assert len(walking_result_bags) == n_osm_nodes

            bags_i[i] = walking_result_bags
            # bags_i[i] = deepcopy(walking_result_bags)

        storage.write_any_dict(
            {
                "bags_i": bags_i,
                "path_manager": self.path_manager,
                "multi_modal_node_to_resetted_map": self.multi_modal_node_to_resetted_map,
                "walking_node_to_resetted_map": self.walking_node_to_resetted_map,
                "stops_df": self.stops_df,
                # "init_walking_result_bags": init_walking_result_bags,
                # "walking_result_bags": walking_result_bags,
                # "bicycle_result_bags": bicycle_result_bags,
                # "mc_raptor_result_bags": mc_raptor_result_bags,
                # "mc_raptor_bags": mc_raptor_bags,
                # "combined_bags": combined_bags,
            },
            output_path,
        )

    def convert_walking_bags(
        self,
        bags: dict[int, list[PyLabel]],
        path_index_offset: int = 0,
        add_zero_weight_to_values: bool = False,
    ) -> IntermediateBags:
        """
        Converts the bags from MLC on a walking graph to the intermediate bags
        format.
        The node ids will be translated from resetted walking node ids to osm
        node ids.

        :param bags: The bags resulting from MLC on a walking graph.
        :param path_index_offset: Used to properly build the path and should
            be set to the length of the path before the step.
        :param add_zero_weight_to_values: Whether to add a zero weight to the
            values of the labels. Needed when adding the 'cost' value after the
            initial walking MLC step.
        """
        converted_bags = convert_mlc_bags_to_intermediate_bags(
            bags,
            translate_node_id=lambda node_id: int(
                self.resetted_to_walking_node_map[node_id][
                    1:
                ]  # remove the 'W' prefix and cast to int
            ),
            add_zero_weight_to_values=add_zero_weight_to_values,
        )
        if self.path_manager:
            self.path_manager.extract_all_paths_from_bags(
                converted_bags,
                PathType.WALKING,
                path_index_offset=path_index_offset,
            )
        return converted_bags

    def convert_bicycle_bags(
        self, bags: dict[int, list[PyLabel]], path_index_offset: int
    ) -> IntermediateBags:
        """
        Converts the bags from MLC on a multi modal graph to the intermediate
        bags format.
        The node ids will be translated from resetted multi modal node ids to
        osm node ids.

        Note that there are two bags per osm node id, one for the walking
        and one for the biking graph. Only the bags for the walking graph
        are used.


        :param bags: The bags resulting from MLC on a multi modal graph.
        :param path_index_offset: Used to properly build the path and should
            be set to the length of the path before the step.
        """
        # only use the bags for walking nodes
        bags = {
            node_id: labels
            for node_id, labels in bags.items()
            if self.resetted_to_multi_modal_node_map[node_id].startswith("W")
        }
        bags = convert_mlc_bags_to_intermediate_bags(
            bags,
            translate_node_id=lambda node_id: int(
                self.resetted_to_multi_modal_node_map[node_id][1:]
            ),
        )
        if self.path_manager:
            self.path_manager.extract_all_paths_from_bags(
                bags,
                PathType.CYCLING_WALKING,
                path_index_offset=path_index_offset,
            )
        return bags

    def convert_mc_raptor_bags(
        self, bags: dict[str, Bag], path_index_offset: int
    ) -> IntermediateBags:
        """
        Converts the bags from the McRAPTOR step to the intermediate bags
        format.
        The stop ids will be translated to the nearest osm node ids.

        :param bags: The bags resulting from MLC on a multi modal graph.
        :param path_index_offset: Used to properly build the path and should
            be set to the length of the path before the step.
        """
        mc_raptor_result_bags = {
            self.stop_to_osm_node_map[int(stop_id)]: bag
            for stop_id, bag in bags.items()
        }
        mc_raptor_result_bags = convert_mc_raptor_bags_to_intermediate_bags(
            mc_raptor_result_bags,
            min_path_length=path_index_offset + 1,
        )
        if self.path_manager:
            self.path_manager.extract_all_paths_from_bags(
                mc_raptor_result_bags,
                PathType.PUBLIC_TRANSPORT,
                path_index_offset=path_index_offset,
            )

        return mc_raptor_result_bags

    def prepare_bicycle_step_input(self, bags: IntermediateBags) -> IntermediateBags:
        def translate_osm_node_id_to_bicycle_node_id(
            osm_node_id: int,
        ) -> int:
            original_bicycle_node = f"B{osm_node_id}"
            bicycle_node_id = self.multi_modal_node_to_resetted_map[
                original_bicycle_node
            ]
            return bicycle_node_id

        # filter bags at bicycle nodes
        bicycle_bags = {
            node_id: bag
            for node_id, bag in bags.items()
            if node_id in self.bicycle_transfer_nodes_walking_node_ids
        }

        # translate node ids
        bicycle_bags = {
            translate_osm_node_id_to_bicycle_node_id(node_id): [
                label.to_mlc_label(translate_osm_node_id_to_bicycle_node_id(node_id))
                for label in labels
            ]
            for node_id, labels in bicycle_bags.items()
        }
        return bicycle_bags

    def prepare_walking_step_input(self, bags: IntermediateBags) -> IntermediateBags:
        def translate_osm_node_id_to_walking_node_id(
            osm_node_id: int,
        ) -> int:
            walking_node_id = f"W{osm_node_id}"
            resetted_walking_node_id = self.walking_node_to_resetted_map[
                walking_node_id
            ]
            return resetted_walking_node_id

        # translate node ids
        walking_bags = {
            translate_osm_node_id_to_walking_node_id(node_id): [
                label.to_mlc_label(
                    translate_osm_node_id_to_walking_node_id(node_id),
                    with_hidden_values=False,
                )
                for label in labels
            ]
            for node_id, labels in bags.items()
        }

        return walking_bags

    # converts bags with node ids to bags with stop ids
    def prepare_mcraptor_step_input(self, bags: IntermediateBags) -> McRAPTORInputBags:
        def translate_osm_node_id_to_stop_id(
            walking_node_id: int,
        ) -> str | None:
            if walking_node_id in self.osm_node_to_stop_map:
                stop_id = str(self.osm_node_to_stop_map[walking_node_id])
                return stop_id
            return None

        mc_raptor_bags = {
            node_id: bag
            for node_id, bag in bags.items()
            if translate_osm_node_id_to_stop_id(node_id) is not None
        }
        mc_raptor_bags_string: McRAPTORInputBags = {
            translate_osm_node_id_to_stop_id(node_id): Bag.from_labels(
                [
                    label.to_mc_raptor_label(
                        translate_osm_node_id_to_stop_id(node_id), null_cost=True  # type: ignore
                    )
                    for label in labels
                ]
            )
            for node_id, labels in mc_raptor_bags.items()
        }

        return mc_raptor_bags_string

    def merge_bags(
        self,
        bags_a: IntermediateBags,
        bags_b: IntermediateBags,
    ) -> IntermediateBags:
        """
        Merges two bag dictionaries into one.
        Expects that bags_a has a bag for every node in bags_b.
        """
        # combined_bags = deepcopy(
        #     bags_a
        # )  # remove deepcopy, if you are sure, that bicycle_result_bags is not used anymore
        combined_bags = bags_a
        for node_id, bag in bags_b.items():
            merged_bag = merge_intermediate_bags(combined_bags[node_id], bag)
            combined_bags[node_id] = merged_bag

        return combined_bags

    def nullify_hidden_values(self, bags: IntermediateBags) -> IntermediateBags:
        """
        Sets the hidden values of the bags to 0.
        Necessary between two multi-modal steps, as ending the multi-modal step
        is equivalent to dismounting.
        """
        # nullify hidden_values
        # this is done to reset the time spent on a bicycle as ending the
        # bicycle step is equivalent to dismounting
        for bag in bags.values():
            for label in bag:
                label.hidden_values = []
        return bags


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
        + ",0)"
    )
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
