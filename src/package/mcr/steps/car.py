from logging import Logger
from typing import Optional
from package import storage
from package.mcr.bag import IntermediateBags
from mcr_py import GraphCache
from package.logger import Timer
from package.mcr.bag import (
    IntermediateBags,
)
from package.mcr.data import (
    AVG_CAR_SPEED,
    DRIVING_PREFIX,
    TRAVEL_TIME_COLUMN,
    TRAVEL_TIME_DRIVING_COLUMN,
    WALKING_PREFIX,
    add_weights,
    create_multi_modal_graph,
    get_reverse_map,
    reset_node_ids,
    to_mlc_edges,
)
from package.mcr.path import PathManager, PathType
from package.mcr.steps.interface import StepBuilder
from package.mcr.steps.mlc import MLCStep
import geopandas as gpd
from package.osm import osm


class PersonalCarStep(MLCStep):
    NAME = "personal car"
    PATH_TYPE = PathType.DRIVING_WALKING

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
        self.update_label_func = "personal_car"
        self.graph_cache = graph_cache
        self.to_internal = to_internal
        self.from_internal = from_internal
        self.valid_end_nodes = from_internal.keys()

        self.valid_starting_nodes = None 

        def nullify_car_hidden_values_cost(
            bags: IntermediateBags,
        ) -> IntermediateBags:
            for bag in bags.values():
                for label in bag:
                    label.hidden_values[0] = 0
            return bags

        self.after_conversion_func = nullify_car_hidden_values_cost


class PersonalCarStepBuilder(StepBuilder):
    step = PersonalCarStep

    def __init__(
        self,
        walking_nodes: gpd.GeoDataFrame,
        walking_edges: gpd.GeoDataFrame,
        driving_nodes: gpd.GeoDataFrame,
        driving_edges: gpd.GeoDataFrame,
        pois: gpd.GeoDataFrame,
    ):
        multi_modal_nodes, multi_modal_edges = create_multi_modal_graph(
            walking_nodes, walking_edges, driving_nodes, driving_edges, AVG_CAR_SPEED
        )

        (
            multi_modal_nodes,
            multi_modal_edges,
            self.multi_modal_node_to_resetted_map,
        ) = reset_node_ids(multi_modal_nodes, multi_modal_edges)

        self.resetted_to_multi_modal_node_map = get_reverse_map(
            self.multi_modal_node_to_resetted_map
        )

        self.osm_node_to_mm_car_resetted_map = {
            int(k[1:]): v
            for k, v in self.multi_modal_node_to_resetted_map.items()
            if k[0] == DRIVING_PREFIX
        }
        self.mm_walking_node_resetted_to_osm_node_map = {
            k: int(v[1:])
            for k, v in self.resetted_to_multi_modal_node_map.items()
            if v[0] == WALKING_PREFIX
        }

        multi_modal_edges = add_weights(multi_modal_edges, [TRAVEL_TIME_COLUMN])
        multi_modal_edges = add_weights(
            multi_modal_edges, [TRAVEL_TIME_DRIVING_COLUMN], hidden=True
        )

        raw_edges = to_mlc_edges(multi_modal_edges)
        self.osm_nodes = walking_nodes
        self.mm_graph_cache = GraphCache()
        self.mm_graph_cache.set_graph(raw_edges)
        self.add_pois_to_mm_graph(pois)

        self.kwargs = {
            "graph_cache": self.mm_graph_cache,
            "to_internal": self.osm_node_to_mm_car_resetted_map,
            "from_internal": self.mm_walking_node_resetted_to_osm_node_map,
        }

    def save_translations(self, output_path: str):
        storage.write_any_dict(
            {
                "resetted_to_multi_modal_node_map": self.resetted_to_multi_modal_node_map,
                "multi_modal_node_to_resetted_map": self.multi_modal_node_to_resetted_map,
            },
            output_path,
        )

    def add_pois_to_mm_graph(self, pois):
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
