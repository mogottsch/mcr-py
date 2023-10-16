from typing import Any, Optional
import sys

import pandas as pd

import mcr_py
from mcr_py import PyLabel
from package import storage, strtime
from package.logger import Timer, rlog
from package.mcr.data import MCRGeoData
from package.mcr.label import (
    IntermediateLabel,
    McRAPTORLabel,
    McRAPTORLabelWithPath,
    merge_intermediate_bags,
)
from package.mcr.output import OutputFormat
from package.mcr.path import PathManager, PathType
from package.raptor.mcraptor_single import McRaptorSingle
from package.raptor.bag import Bag
from package.mcr.bag import (
    IntermediateBags,
    convert_mc_raptor_bags_to_intermediate_bags,
    convert_mlc_bags_to_intermediate_bags,
)

McRAPTORInputBags = dict[str, list[IntermediateLabel]]


class MCR:
    def __init__(
        self,
        mcr_geo_data: MCRGeoData,
        disable_paths: bool = False,
        output_format: OutputFormat = OutputFormat.CLASS_PICKLE,
        bicycle_price_function: str = "next_bike_no_tariff",
        logger=rlog,
        enable_limit: bool = False,
    ):
        self.geo_data = mcr_geo_data
        self.disable_paths = disable_paths
        self.path_manager: Optional[PathManager] = None
        self.output_format = output_format
        self.bicycle_price_function = bicycle_price_function
        self.logger = logger
        self.timer = Timer(logger)
        self.enable_limit = enable_limit
        if not disable_paths:
            self.path_manager = PathManager()

    def run(
        self, start_node_id: int, start_time: str, max_transfers: int, output_path: str
    ):
        start_time_in_seconds = strtime.str_time_to_seconds(start_time)

        bags_i: dict[int, IntermediateBags] = {}

        self.logger.debug(f"Starting MCR with config: {self.__dict__}")

        with self.timer.info("Running Dijkstra step"):
            walking_result_bags = mcr_py.run_mlc_with_node_and_time(
                self.geo_data.walking_graph_cache,
                self.geo_data.walking_node_to_resetted_map[f"W{start_node_id}"],
                start_time_in_seconds,
                disable_paths=self.disable_paths,
                enable_limit=self.enable_limit,
            )

        with self.timer.info("Extracting dijkstra step bags"):
            walking_result_bags = self.convert_walking_bags(
                walking_result_bags,
            )
            bags_i[0] = walking_result_bags

            n_osm_nodes = len(walking_result_bags)
            self.logger.debug(f"Found {n_osm_nodes} osm nodes in walking step")

        for i in range(1, max_transfers + 1):
            self.logger.info(f"Running iteration {i}")
            offset = i * 2 - 1

            bicycle_result_bags = self.run_bicycle_step(walking_result_bags, offset)
            mc_raptor_result_bags = self.run_mc_raptor_step(walking_result_bags, offset)

            with self.timer.info("Merging bags"):
                combined_bags = self.merge_bags(
                    bicycle_result_bags,
                    mc_raptor_result_bags,
                )

            with self.timer.info("Preparing input for walking step"):
                walking_bags = self.prepare_walking_step_input(combined_bags)

            with self.timer.info("Running walking step"):
                walking_result_bags = mcr_py.run_mlc_with_bags(
                    self.geo_data.walking_graph_cache,
                    walking_bags,
                    disable_paths=self.disable_paths,
                    enable_limit=self.enable_limit,
                )
            with self.timer.info("Extracting walking step bags"):
                walking_result_bags = self.convert_walking_bags(
                    walking_result_bags, path_index_offset=offset + 1
                )

            bags_i[i] = walking_result_bags

        with self.timer.info("Saving bags"):
            self.save_bags(bags_i, output_path)

    def run_bicycle_step(self, walking_result_bags: IntermediateBags, offset: int):
        with self.timer.info("Preparing input for bicycle step"):
            bicycle_input_bags = self.prepare_bicycle_step_input(walking_result_bags)
            if len(bicycle_input_bags) == 0 and self.enable_limit:
                self.logger.warn("No bicycles reached with walking step")
                return walking_result_bags

        with self.timer.info("Running bicycle step"):
            bicycle_result_bags = mcr_py.run_mlc_with_bags(
                self.geo_data.mm_graph_cache,
                bicycle_input_bags,
                update_label_func=self.bicycle_price_function,
                disable_paths=self.disable_paths,
                enable_limit=self.enable_limit,
            )

        with self.timer.info("Extracting bicycle step bags"):
            bicycle_result_bags = self.convert_bicycle_bags(
                bicycle_result_bags, path_index_offset=offset
            )

        return bicycle_result_bags

    def run_mc_raptor_step(
        self, walking_result_bags: IntermediateBags, offset: int
    ) -> IntermediateBags:
        with self.timer.info("Preparing input for MCRAPTOR step"):
            mc_raptor_bags = self.prepare_mcraptor_step_input(walking_result_bags)

        with self.timer.info("Running MCRAPTOR step"):
            mc_raptor = McRaptorSingle(
                self.geo_data.structs_dict,
                default_transfer_time=60,
                label_class=(McRAPTORLabel
                if self.disable_paths
                else McRAPTORLabelWithPath),
            )
            mc_raptor_result_bags = mc_raptor.run(mc_raptor_bags)  # type: ignore

        with self.timer.info("Extracting MCRAPTOR step bags"):
            mc_raptor_result_bags = self.convert_mc_raptor_bags(
                mc_raptor_result_bags, path_index_offset=offset
            )
            if len(mc_raptor_result_bags) == 0:
                self.logger.warn("No MCRAPTOR bags found")

        return mc_raptor_result_bags

    def save_bags(
        self,
        bags_i: dict[int, IntermediateBags],
        output_path: str,
    ):
        if self.output_format == OutputFormat.CLASS_PICKLE:
            self.save_pickle(bags_i, output_path)
        elif self.output_format == OutputFormat.DF_FEATHER:
            self.save_feather(bags_i, output_path)

    def save_pickle(self, bags_i: dict[int, IntermediateBags], output_path: str):
        results: dict[str, Any] = {
            "bags_i": bags_i,
        }

        if not self.disable_paths:
            results["path_manager"] = self.path_manager
            results[
                "multi_modal_node_to_resetted_map"
            ] = self.geo_data.multi_modal_node_to_resetted_map
            results[
                "walking_node_to_resetted_map"
            ] = self.geo_data.walking_node_to_resetted_map
            results["stops_df"] = self.geo_data.stops_df
        storage.write_any_dict(
            results,
            output_path,
        )

    def save_feather(self, bags_i: dict[int, IntermediateBags], output_path: str):
        labels = pd.DataFrame(
            [
                (label.node_id, label.values[0], label.values[1], n_transfers)
                for n_transfers, bags in bags_i.items()
                for bag in bags.values()
                for label in bag
            ],
            columns=["osm_node_id", "time", "cost", "n_transfers"],
        )

        labels["human_readable_time"] = labels["time"].apply(
            strtime.seconds_to_str_time
        )

        labels.to_feather(output_path)

    def convert_walking_bags(
        self,
        bags: dict,
        path_index_offset: int = 0,
    ) -> IntermediateBags:
        """
        Converts the bags from MLC on a walking graph to the intermediate bags
        format.
        The node ids will be translated from resetted walking node ids to osm
        node ids.

        :param bags: The bags resulting from MLC on a walking graph.
        :param path_index_offset: Used to properly build the path and should
            be set to the length of the path before the step.
        """
        converted_bags = convert_mlc_bags_to_intermediate_bags(
            bags,
            translate_node_id=lambda node_id: int(
                self.geo_data.resetted_to_walking_node_map[node_id][
                    1:
                ]  # remove the 'W' prefix and cast to int
            ),
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
            if self.geo_data.resetted_to_multi_modal_node_map[node_id].startswith("W")
        }
        bags = convert_mlc_bags_to_intermediate_bags(
            bags,
            translate_node_id=lambda node_id: int(
                self.geo_data.resetted_to_multi_modal_node_map[node_id][1:]
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
            self.geo_data.stop_to_osm_node_map[int(stop_id)]: bag
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
            bicycle_node_id = self.geo_data.multi_modal_node_to_resetted_map[
                original_bicycle_node
            ]
            return bicycle_node_id

        # filter bags at bicycle nodes
        bicycle_bags = {
            node_id: bag
            for node_id, bag in bags.items()
            if node_id in self.geo_data.bicycle_transfer_nodes_walking_node_ids
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
            resetted_walking_node_id = self.geo_data.walking_node_to_resetted_map[
                walking_node_id
            ]
            return resetted_walking_node_id

        walking_bags = {
            translate_osm_node_id_to_walking_node_id(node_id): [
                label.to_mlc_label(
                    translate_osm_node_id_to_walking_node_id(node_id),
                )
                for label in labels
            ]
            for node_id, labels in bags.items()
        }

        # nullify time spent on bicycle
        for bag in walking_bags.values():
            for label in bag:
                label.hidden_values[0] = 0

        return walking_bags

    # converts bags with node ids to bags with stop ids
    def prepare_mcraptor_step_input(self, bags: IntermediateBags) -> McRAPTORInputBags:
        def translate_osm_node_id_to_stop_id(
            walking_node_id: int,
        ) -> str | None:
            if walking_node_id in self.geo_data.osm_node_to_stop_map:
                stop_id = str(self.geo_data.osm_node_to_stop_map[walking_node_id])
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
                        translate_osm_node_id_to_stop_id(node_id),  # type: ignore
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
        """
        combined_bags = bags_a
        for node_id, b_bag in bags_b.items():
            a_bag = combined_bags.get(node_id, [])
            merged_bag = merge_intermediate_bags(a_bag, b_bag)
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


# def get_graph(
#     osm_reader: pyrosm.OSM, geo_meta: GeoMeta
# ) -> Tuple[gpd.GeoDataFrame, gpd.GeoDataFrame]:
#     with Timed.info("Getting OSM graph"):
#         nodes, edges = osm.get_graph_for_city_cropped_to_boundary(osm_reader, stops_df)
#
#     return nodes, edges


def get_size(obj: Any) -> int:
    size = sys.getsizeof(obj)

    if isinstance(obj, dict):
        for key, value in obj.items():
            size += get_size(key) + get_size(value)

    elif isinstance(obj, list):
        size += sum([get_size(i) for i in obj])

    elif isinstance(obj, tuple):
        size += sum([get_size(i) for i in obj])

    elif hasattr(obj, "__dict__"):
        size += get_size(obj.__dict__)

    return size


def pretty_bytes(b: float) -> str:
    for unit in ["B", "KB", "MB", "GB", "TB", "PB"]:
        if b < 1024:
            return f"{b:.2f} {unit}"
        b /= 1024
    return f"{b:.2f} PB"
