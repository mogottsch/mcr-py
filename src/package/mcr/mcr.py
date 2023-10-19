from typing import Any, Optional
import sys

import pandas as pd

from package import storage, strtime
from package.mcr.config import MCRConfig
from package.mcr.data import MCRGeoData
from package.mcr.label import (
    IntermediateLabel,
    merge_intermediate_bags,
)
from package.mcr.output import OutputFormat
from package.mcr.path import PathManager
from package.mcr.steps.public_transport import PublicTransportStep
from package.mcr.bag import IntermediateBags

from package.mcr.steps.bicycle import BicycleStep, BicycleStepBuilder
from package.mcr.steps.walking import WalkingStep


class MCR:
    def __init__(
        self,
        mcr_geo_data: MCRGeoData,
        config: MCRConfig = MCRConfig(),
        output_format: OutputFormat = OutputFormat.CLASS_PICKLE,
        bicycle_price_function: str = "next_bike_no_tariff",
    ):
        self.geo_data = mcr_geo_data
        self.disable_paths = config.disable_paths
        self.path_manager: Optional[PathManager] = None
        if not self.disable_paths:
            self.path_manager = PathManager()
        self.output_format = output_format
        self.bicycle_price_function = bicycle_price_function
        self.logger = config.logger
        self.timer = config.timer
        self.enable_limit = config.enable_limit

    def run(
        self, start_node_id: int, start_time: str, max_transfers: int, output_path: str
    ):
        start_time_in_seconds = strtime.str_time_to_seconds(start_time)

        bicycle_step_builder = BicycleStepBuilder(
            self.geo_data.mm_graph_cache,
            self.geo_data.multi_modal_node_to_resetted_map,
            self.geo_data.resetted_to_multi_modal_node_map,
            self.geo_data.bicycle_transfer_nodes_walking_node_ids,
            self.bicycle_price_function,
        )
        bicycle_step = bicycle_step_builder.build(
            self.logger,
            self.timer,
            self.path_manager,
            self.enable_limit,
            self.disable_paths,
        )
        self.logger.debug(f"Bicycle step: {bicycle_step}")
        public_transport_step = PublicTransportStep(
            self.logger,
            self.timer,
            self.path_manager,
            self.enable_limit,
            self.disable_paths,
            self.geo_data.structs_dict,
            self.geo_data.osm_node_to_stop_map,
            self.geo_data.stop_to_osm_node_map,
        )

        walking_step = WalkingStep(
            self.logger,
            self.timer,
            self.path_manager,
            self.enable_limit,
            self.disable_paths,
            self.geo_data.walking_graph_cache,
            self.geo_data.walking_node_to_resetted_map,
            self.geo_data.resetted_to_walking_node_map,
        )

        bags_i: dict[int, IntermediateBags] = {}

        self.logger.debug(f"Starting MCR with config: {self.__dict__}")

        start_bags = self.create_start_bags(start_node_id, start_time_in_seconds)

        walking_result_bags = walking_step.run(start_bags)

        bags_i[0] = walking_result_bags

        for i in range(1, max_transfers + 1):
            self.logger.info(f"Running iteration {i}")
            offset = i * 2 - 1

            bicycle_result_bags = bicycle_step.run(walking_result_bags, offset)
            public_transport_result_bags = public_transport_step.run(
                walking_result_bags, offset
            )
            with self.timer.info("Merging bags"):
                combined_bags = self.merge_bags(
                    bicycle_result_bags,
                    public_transport_result_bags,
                )

            walking_result_bags = walking_step.run(combined_bags, offset + 1)

            bags_i[i] = walking_result_bags

        with self.timer.info("Saving bags"):
            self.save_bags(bags_i, output_path)

    def create_start_bags(
        self, start_node_id: int, start_time: int
    ) -> IntermediateBags:
        return {
            start_node_id: [
                IntermediateLabel(
                    values=[start_time, 0],
                    hidden_values=[0, 0],
                    path=[] if self.disable_paths else [start_node_id],
                    osm_node_id=start_node_id,
                )
            ]
        }

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
