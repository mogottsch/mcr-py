from logging import Logger
from typing import Callable, Optional
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


class MLCStep(Step):
    NAME = "mlc"

    def __init__(
        self,
        logger: Logger,
        timer: Timer,
        path_manager: Optional[PathManager],
        enable_limit: bool,
        disable_paths: bool,
        graph_cache: GraphCache,
        to_internal: dict,
        from_internal: dict,
    ):
        self.logger = logger
        self.timer = timer
        self.path_manager = path_manager
        self.enable_limit = enable_limit
        self.disable_paths = disable_paths
        self.graph_cache = graph_cache
        self.to_internal = to_internal
        self.from_internal = from_internal

        self.update_label_func: Optional[str] = None
        self.valid_starting_nodes: Optional[set] = None
        self.valid_end_nodes: Optional[set] = None

        self.after_conversion_func: Optional[
            Callable[[IntermediateBags], IntermediateBags]
        ] = None

    def run(self, input_bags: IntermediateBags, offset: int = 0) -> IntermediateBags:
        with self.timer.info(f"Preparing input for {self.NAME} step"):
            prepared_input_bags = self.prepare_input(input_bags)

        with self.timer.info(f"Running {self.NAME} step"):
            raw_result_bags = mcr_py.run_mlc_with_bags(
                self.graph_cache,
                prepared_input_bags,
                update_label_func=self.update_label_func,
                disable_paths=self.disable_paths,
                enable_limit=self.enable_limit,
            )
        with self.timer.info(f"Extracting {self.NAME} step bags"):
            converted_result_bags = self.convert_bags(
                raw_result_bags, path_index_offset=offset
            )

        return converted_result_bags

    def prepare_input(self, bags: IntermediateBags) -> IntermediateBags:
        if self.valid_starting_nodes is not None:
            bags = {
                node_id: bag
                for node_id, bag in bags.items()
                if node_id in self.valid_starting_nodes
            }

        bags = {
            self.to_internal[node_id]: [
                label.to_mlc_label(
                    self.to_internal[node_id],
                )
                for label in labels
            ]
            for node_id, labels in bags.items()
        }

        return bags

    def convert_bags(
        self,
        bags: dict,
        path_index_offset: int = 0,
    ) -> IntermediateBags:
        if self.valid_end_nodes is not None:
            bags = {
                node_id: labels
                for node_id, labels in bags.items()
                if node_id in self.valid_end_nodes
            }

        converted_bags = convert_mlc_bags_to_intermediate_bags(
            bags,
            translate_node_id=lambda node_id: self.from_internal[node_id],
        )

        if self.path_manager:
            self.path_manager.extract_all_paths_from_bags(
                converted_bags,
                PathType.WALKING,
                path_index_offset=path_index_offset,
            )

        if self.after_conversion_func:
            converted_bags = self.after_conversion_func(converted_bags)

        return converted_bags
