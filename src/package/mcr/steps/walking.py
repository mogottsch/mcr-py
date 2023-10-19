from logging import Logger
from typing import Optional
from package.logger import Timer
from package.mcr.bag import IntermediateBags
from package.mcr.path import PathManager, PathType
from package.mcr.steps.interface import Step
from mcr_py import GraphCache
import mcr_py
from package.mcr.bag import (
    IntermediateBags,
    convert_mlc_bags_to_intermediate_bags,
)


class WalkingStep(Step):
    def __init__(
        self,
        logger: Logger,
        timer: Timer,
        path_manager: Optional[PathManager],
        enable_limit: bool,
        disable_paths: bool,
        graph_cache: GraphCache,
        walking_node_to_resetted_map: dict,
        resetted_to_walking_node_map: dict,
    ):
        self.logger = logger
        self.timer = timer
        self.path_manager = path_manager
        self.enable_limit = enable_limit
        self.disable_paths = disable_paths
        self.graph_cache = graph_cache
        self.walking_node_to_resetted_map = walking_node_to_resetted_map
        self.resetted_to_walking_node_map = resetted_to_walking_node_map

    def run(self, input_bags: IntermediateBags, offset: int = 0) -> IntermediateBags:
        with self.timer.info("Preparing input for walking step"):
            prepared_input_bags = self.prepare_walking_step_input(input_bags)

        with self.timer.info("Running walking step"):
            raw_walking_result_bags = mcr_py.run_mlc_with_bags(
                self.graph_cache,
                prepared_input_bags,
                disable_paths=self.disable_paths,
                enable_limit=self.enable_limit,
            )
        with self.timer.info("Extracting walking step bags"):
            converted_walking_result_bags = self.convert_walking_bags(
                raw_walking_result_bags, path_index_offset=offset
            )

        return converted_walking_result_bags

    def prepare_walking_step_input(self, bags: IntermediateBags) -> IntermediateBags:
        def translate_osm_node_id_to_walking_node_id(
            osm_node_id: int,
        ) -> int:
            walking_node_id = f"W{osm_node_id}"
            resetted_walking_node_id = self.walking_node_to_resetted_map[
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

        # TODO: this does not belong here
        # nullify time spent on bicycle
        for bag in walking_bags.values():
            for label in bag:
                label.hidden_values[0] = 0

        return walking_bags

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
                self.resetted_to_walking_node_map[node_id][
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
