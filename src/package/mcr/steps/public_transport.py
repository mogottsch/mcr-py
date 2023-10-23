from logging import Logger
from typing import Optional
from package import storage
from package.logger import Timed, Timer
from package.mcr.bag import IntermediateBags
from package.mcr.label import IntermediateLabel, McRAPTORLabel, McRAPTORLabelWithPath
from package.mcr.path import PathManager, PathType
from package.mcr.steps.interface import Step, StepBuilder
from package.osm import graph
from package.raptor.bag import Bag
from package.mcr.bag import (
    IntermediateBags,
    convert_mc_raptor_bags_to_intermediate_bags,
)
from package.raptor.mcraptor_single import McRaptorSingle
import networkx as nx

McRAPTORInputBags = dict[str, list[IntermediateLabel]]


class PublicTransportStep(Step):
    def __init__(
        self,
        logger: Logger,
        timer: Timer,
        path_manager: Optional[PathManager],
        enable_limit: bool,
        disable_paths: bool,
        structs_dict: dict,
        osm_node_to_stop_map: dict[int, int],
        stop_to_osm_node_map: dict[int, int],
    ):
        self.logger = logger
        self.timer = timer
        self.path_manager = path_manager
        self.enable_limit = enable_limit
        self.disable_paths = disable_paths
        self.structs_dict = structs_dict
        self.osm_node_to_stop_map = osm_node_to_stop_map
        self.stop_to_osm_node_map = stop_to_osm_node_map

    def run(self, input_bags: IntermediateBags, offset: int) -> IntermediateBags:
        with self.timer.info("Preparing input for MCRAPTOR step"):
            prepared_input_bags = self.prepare_public_transport_step_input(input_bags)
            if len(prepared_input_bags) == 0:
                self.logger.warn(
                    "Not a single stop is reached by the previous step - aborting MCRAPTOR step"
                )
                return {}

        with self.timer.info("Running MCRAPTOR step"):
            mc_raptor = McRaptorSingle(
                self.structs_dict,
                default_transfer_time=60,
                label_class=(
                    McRAPTORLabel if self.disable_paths else McRAPTORLabelWithPath
                ),
            )
            raw_public_transport_result_bags = mc_raptor.run(prepared_input_bags)  # type: ignore

        with self.timer.info("Extracting MCRAPTOR step bags"):
            raw_public_transport_result_bags = self.convert_public_transport_bags(
                raw_public_transport_result_bags, path_index_offset=offset
            )
            if len(raw_public_transport_result_bags) == 0:
                self.logger.warn("No MCRAPTOR bags found")

        return raw_public_transport_result_bags

    # converts bags with node ids to bags with stop ids
    def prepare_public_transport_step_input(
        self, bags: IntermediateBags
    ) -> McRAPTORInputBags:
        def translate_osm_node_id_to_stop_id(
            walking_node_id: int,
        ) -> str | None:
            if walking_node_id in self.osm_node_to_stop_map:
                stop_id = str(self.osm_node_to_stop_map[walking_node_id])
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

    def convert_public_transport_bags(
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
            self.stop_to_osm_node_map[int(stop_id)]: bag
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


class PublicTransportStepBuilder(StepBuilder):
    step = PublicTransportStep

    def __init__(
        self,
        structs_path: str,
        stops_path: str,
        nxgraph: nx.Graph,
    ):
        structs_dict = storage.read_any_dict(structs_path)
        with Timed.info("Reading stops"):
            self.stops_df = storage.read_gdf(stops_path)

        stops_df = graph.add_nearest_node_to_stops(self.stops_df, nxgraph)

        stops_df["stop_id"] = stops_df["stop_id"].astype(int)
        stop_to_osm_node_map: dict[int, int] = stops_df.set_index("stop_id")[
            "nearest_node"
        ].to_dict()
        osm_node_to_stop_map: dict[int, int] = {
            v: k for k, v in stop_to_osm_node_map.items()
        }
        self.kwargs = {
            "structs_dict": structs_dict,
            "osm_node_to_stop_map": osm_node_to_stop_map,
            "stop_to_osm_node_map": stop_to_osm_node_map,
        }
