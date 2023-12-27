from logging import Logger
import numpy as np
from typing import Optional
from package import storage
from package.geometa import GeoMeta
from package.mcr.bag import IntermediateBags
from mcr_py import GraphCache
from package.logger import Timer
from package.mcr.bag import (
    IntermediateBags,
)
from package.mcr.data import (
    AVG_BIKING_SPEED,
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
from package.logger import rlog


class BicycleStep(MLCStep):
    NAME = "bicycle"
    PATH_TYPE = PathType.CYCLING_WALKING

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
        update_label_func: str,
        bicycle_location_path: str,
        geo_meta: GeoMeta,
        walking_nodes: gpd.GeoDataFrame,
        walking_edges: gpd.GeoDataFrame,
        cycling_nodes: gpd.GeoDataFrame,
        cycling_edges: gpd.GeoDataFrame,
        pois: gpd.GeoDataFrame,
    ):
        bicycle_locations = None
        if bicycle_location_path != "":
            bicycle_locations = storage.read_df(bicycle_location_path)
            bicycle_locations = gpd.GeoDataFrame(
                bicycle_locations,
                geometry=gpd.points_from_xy(
                    bicycle_locations.lon,
                    bicycle_locations.lat,
                ),
            )
            bicycle_locations = geo_meta.crop_gdf(bicycle_locations)
            bicycle_locations = osm.add_nearest_osm_node_id(
                bicycle_locations, cycling_nodes
            )
        else:
            rlog.warn("No bicycle locations provided - will use random locations")

        if bicycle_locations is not None:
            cycling_nodes = mark_bicycles(cycling_nodes, bicycle_locations)
        else:
            cycling_nodes = mark_bicycles_random(cycling_nodes, 100)

        bicycle_transfer_osm_node_ids = cycling_nodes[
            cycling_nodes["has_bicycle"]
        ].id.values

        multi_modal_nodes, multi_modal_edges = create_multi_modal_graph(
            walking_nodes, walking_edges, cycling_nodes, cycling_edges, AVG_BIKING_SPEED
        )

        (
            multi_modal_nodes,
            multi_modal_edges,
            self.multi_modal_node_to_resetted_map,
        ) = reset_node_ids(multi_modal_nodes, multi_modal_edges)

        self.resetted_to_multi_modal_node_map = get_reverse_map(
            self.multi_modal_node_to_resetted_map
        )

        self.osm_node_to_mm_bicycle_resetted_map = {
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
            "to_internal": self.osm_node_to_mm_bicycle_resetted_map,
            "from_internal": self.mm_walking_node_resetted_to_osm_node_map,
            "bicycle_transfer_osm_node_ids": bicycle_transfer_osm_node_ids,
            "update_label_func": update_label_func,
        }

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


def mark_bicycles(
    nodes: gpd.GeoDataFrame,
    bicycle_locations: gpd.GeoDataFrame,
) -> gpd.GeoDataFrame:
    nodes["has_bicycle"] = False

    nodes.loc[bicycle_locations["nearest_osm_node_id"], "has_bicycle"] = True

    return nodes


def mark_bicycles_random(nodes: gpd.GeoDataFrame, n: int) -> gpd.GeoDataFrame:
    nodes["has_bicycle"] = False

    nodes.loc[nodes.sample(n).index, "has_bicycle"] = True

    return nodes
