from logging import Logger
import numpy as np
from typing import Optional
from package.mcr.bag import IntermediateBags
from mcr_py import GraphCache
from package.logger import Timer
from package.mcr.bag import (
    IntermediateBags,
)
from package.mcr.path import PathManager
from package.mcr.steps.interface import StepBuilder
from package.mcr.steps.mlc import MLCStep


class BicycleStep(MLCStep):
    NAME = "bicycle"

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
        bicycle_transfer_osm_node_ids: np.ndarray,
        update_label_func: str,
    ):
        self.logger = logger
        self.timer = timer
        self.path_manager = path_manager
        self.enable_limit = enable_limit
        self.disable_paths = disable_paths
        self.update_label_func = update_label_func
        self.graph_cache = graph_cache
        self.to_internal = to_internal
        self.from_internal = from_internal
        self.valid_starting_nodes = bicycle_transfer_osm_node_ids
        self.valid_end_nodes = from_internal.keys()

        def nullify_bicycle_hidden_values_cost(
            bags: IntermediateBags,
        ) -> IntermediateBags:
            for bag in bags.values():
                for label in bag:
                    label.hidden_values[0] = 0
            return bags

        self.after_conversion_func = nullify_bicycle_hidden_values_cost


class BicycleStepBuilder(StepBuilder):
    step = BicycleStep

    def __init__(
        self,
        graph_cache: GraphCache,
        to_internal: dict,
        from_internal: dict,
        bicycle_transfer_osm_node_ids: np.ndarray,
        update_label_func: str,
    ):
        self.kwargs = {
            "graph_cache": graph_cache,
            "to_internal": to_internal,
            "from_internal": from_internal,
            "bicycle_transfer_osm_node_ids": bicycle_transfer_osm_node_ids,
            "update_label_func": update_label_func,
        }
