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
from package.mcr.steps.public_transport import (
    PublicTransportStep,
    PublicTransportStepBuilder,
)
from package.mcr.bag import IntermediateBags

from package.mcr.steps.bicycle import BicycleStep, BicycleStepBuilder
from package.mcr.steps.walking import WalkingStep, WalkingStepBuilder


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
        public_transport_step_builder = PublicTransportStepBuilder(
            self.geo_data.structs_dict,
            self.geo_data.osm_node_to_stop_map,
            self.geo_data.stop_to_osm_node_map,
        )
        walking_step_builder = WalkingStepBuilder(
            self.geo_data.walking_graph_cache,
            self.geo_data.walking_node_to_resetted_map,
            self.geo_data.resetted_to_walking_node_map,
        )

        builder_kwargs = {
            "logger": self.logger,
            "timer": self.timer,
            "path_manager": self.path_manager,
            "enable_limit": self.enable_limit,
            "disable_paths": self.disable_paths,
        }
        bicycle_step = bicycle_step_builder.build(**builder_kwargs)
        public_transport_step = public_transport_step_builder.build(**builder_kwargs)
        walking_step = walking_step_builder.build(**builder_kwargs)

        initial_steps = [[walking_step]]
        repeating_steps = [
            [bicycle_step, public_transport_step],
            [walking_step],
        ]

        bags_i: dict[int, IntermediateBags] = {}

        self.logger.debug(f"Starting MCR with config: {self.__dict__}")

        start_bags = self.create_start_bags(start_node_id, start_time_in_seconds)

        for steps in initial_steps:
            result_bags = []
            for step in steps:
                result_bags.append(step.run(start_bags))
            start_bags = self.merge_bags(*result_bags)

        bags_i[0] = start_bags

        for i in range(1, max_transfers + 1):
            self.logger.info(f"Running iteration {i}")
            offset = i * 2 - 1

            repeated_games = bags_i[i - 1]
            for steps in repeating_steps:
                result_bags = []
                for step in steps:
                    result_bags.append(step.run(repeated_games, offset))
                repeated_games = self.merge_bags(*result_bags)

            bags_i[i] = repeated_games

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
        *bag_collection: IntermediateBags,
    ) -> IntermediateBags:
        combined_bags = bag_collection[0]

        if len(bag_collection) == 1:
            return combined_bags

        with self.timer.info(f"Merging bags from {len(bag_collection)} steps"):
            for bags in bag_collection[1:]:
                for node_id, bag in bags.items():
                    a_bag = combined_bags.get(node_id, [])
                    merged_bag = merge_intermediate_bags(a_bag, bag)
                    combined_bags[node_id] = merged_bag

        return combined_bags
