from package.osm import osm
from package.logger import Timed
from package.mcr.data import (
    add_weights,
    create_walking_graph,
    get_reverse_map,
    reset_node_ids,
)
from package.mcr.steps.interface import StepBuilder
from mcr_py import GraphCache
from package.mcr.steps.mlc import MLCStep
import pandas as pd


class WalkingStep(MLCStep):
    NAME = "walking"


class WalkingStepBuilder(StepBuilder):
    step = WalkingStep

    def __init__(
        self,
        osm_nodes: pd.DataFrame,
        osm_edges: pd.DataFrame,
        pois: pd.DataFrame,
    ):
        walking_nodes, walking_edges = create_walking_graph(osm_nodes, osm_edges)

        (
            walking_nodes,
            walking_edges,
            self.walking_node_to_resetted_map,
        ) = reset_node_ids(walking_nodes, walking_edges)

        self.resetted_to_walking_node_map = get_reverse_map(
            self.walking_node_to_resetted_map
        )

        self.walking_edges = add_weights(walking_edges, ["travel_time"])
        self.walking_edges = add_weights(walking_edges, [], hidden=True)

        raw_walking_edges = walking_edges[
            ["u", "v", "weights", "hidden_weights"]
        ].to_dict(
            "records"  # type: ignore
        )
        self.osm_nodes = osm_nodes

        self.walking_graph_cache = GraphCache()
        self.walking_graph_cache.set_graph(raw_walking_edges)  # type: ignore
        self.add_pois_to_walking_graph(pois)

        self.kwargs = {
            "graph_cache": self.walking_graph_cache,
            "to_internal": self.walking_node_to_resetted_map,
            "from_internal": self.resetted_to_walking_node_map,
        }

    def add_pois_to_walking_graph(self, pois: pd.DataFrame) -> None:
        """
        Adds POIs to the walking graph cache.

        Args:
            pois: A dataframe containing POIs. Must have the columns "nearest_osm_node_id" and "type".
        """
        osm_nodes = osm.list_column_to_osm_nodes(self.osm_nodes, pois, "type")
        type_map: dict[str, int] = {}
        for t in pois["type"].unique():
            type_map[t] = len(type_map)

        osm_nodes["type_internal"] = osm_nodes["type"].map(
            lambda x: list(map(type_map.get, x))
        )
        osm_nodes["resetted_walking_node_id"] = osm_nodes["id"].map(
            self.walking_node_to_resetted_map  # type: ignore
        )

        resetted_walking_node_id_to_type_map = (
            osm_nodes[["resetted_walking_node_id", "type_internal"]].set_index(
                "resetted_walking_node_id"
            )["type_internal"]
        ).to_dict()

        self.walking_graph_cache.set_node_weights(resetted_walking_node_id_to_type_map)
