from logging import Logger
import numpy as np
from typing import Optional
from package.mcr.bag import IntermediateBags
import mcr_py
from mcr_py import GraphCache, PyLabel
from package.logger import Timer
from package.mcr.bag import (
    IntermediateBags,
    convert_mlc_bags_to_intermediate_bags,
)
from package.mcr.path import PathManager, PathType
from package.mcr.steps.interface import Step, StepBuilder


class BicycleStep(Step):
    def __init__(
        self,
        logger: Logger,
        timer: Timer,
        path_manager: Optional[PathManager],
        enable_limit: bool,
        disable_paths: bool,
        graph_cache: GraphCache,
        multi_modal_node_to_resetted_map: dict,
        resetted_to_multi_modal_node_map: dict,
        bicycle_transfer_nodes_walking_node_ids: np.ndarray,
        update_label_func: str,
    ):
        self.logger = logger
        self.timer = timer
        self.path_manager = path_manager
        self.enable_limit = enable_limit
        self.disable_paths = disable_paths
        self.update_label_func = update_label_func
        self.graph_cache = graph_cache
        self.multi_modal_node_to_resetted_map = multi_modal_node_to_resetted_map
        self.resetted_to_multi_modal_node_map = resetted_to_multi_modal_node_map
        self.bicycle_transfer_nodes_walking_node_ids = (
            bicycle_transfer_nodes_walking_node_ids
        )

    def __str__(self):
        return str(
            {
                "enable_limit": self.enable_limit,
                "disable_paths": self.disable_paths,
                "update_label_func": self.update_label_func,
            }
        )

    def run(self, input_bags: IntermediateBags, offset: int):
        with self.timer.info("Preparing input for bicycle step"):
            prepared_input_bags = self.prepare_bicycle_step_input(input_bags)
            if len(prepared_input_bags) == 0 and self.enable_limit:
                self.logger.warn("No bicycles reached with walking step")
                return input_bags

        with self.timer.info("Running bicycle step"):
            raw_result_bags = mcr_py.run_mlc_with_bags(
                self.graph_cache,
                prepared_input_bags,
                update_label_func=self.update_label_func,
                disable_paths=self.disable_paths,
                enable_limit=self.enable_limit,
            )

        with self.timer.info("Extracting bicycle step bags"):
            converted_result_bags = self.convert_bicycle_bags(
                raw_result_bags, path_index_offset=offset
            )

        return converted_result_bags

    def prepare_bicycle_step_input(self, bags: IntermediateBags) -> IntermediateBags:
        """
        Prepares the bags for the bicycle step by translating the node ids and
        only considering labels at bicycle nodes.

        :param bags: Bags with osm node ids.
        :return: Bags with resetted multi modal node ids.
        """

        def translate_osm_node_id_to_bicycle_node_id(
            osm_node_id: int,
        ) -> int:
            original_bicycle_node = f"B{osm_node_id}"
            bicycle_node_id = self.multi_modal_node_to_resetted_map[
                original_bicycle_node
            ]
            return bicycle_node_id

        # filter bags at bicycle nodes
        filtered_bags = {
            node_id: bag
            for node_id, bag in bags.items()
            if node_id in self.bicycle_transfer_nodes_walking_node_ids
        }

        # translate node ids
        translated_bags = {
            translate_osm_node_id_to_bicycle_node_id(node_id): [
                label.to_mlc_label(translate_osm_node_id_to_bicycle_node_id(node_id))
                for label in labels
            ]
            for node_id, labels in filtered_bags.items()
        }
        return translated_bags

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

        # nullify time spent on bicycle
        for bag in bags.values():
            for label in bag:
                label.hidden_values[0] = 0

        return bags


class BicycleStepBuilder(StepBuilder):
    step = BicycleStep

    def __init__(
        self,
        graph_cache: GraphCache,
        multi_modal_node_to_resetted_map: dict,
        resetted_to_multi_modal_node_map: dict,
        bicycle_transfer_nodes_walking_node_ids: np.ndarray,
        update_label_func: str,
    ):
        self.kwargs = {
            "graph_cache": graph_cache,
            "multi_modal_node_to_resetted_map": multi_modal_node_to_resetted_map,
            "resetted_to_multi_modal_node_map": resetted_to_multi_modal_node_map,
            "bicycle_transfer_nodes_walking_node_ids": (
                bicycle_transfer_nodes_walking_node_ids
            ),
            "update_label_func": update_label_func,
        }
