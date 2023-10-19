from typing_extensions import Annotated

import typer
from pyrosm.data import os

from command.footpaths import CITY_ID_HELP, OSM_HELP, STOPS_HELP
from command.raptor import STRUCTS_HELP
from package import storage
from package.geometa import GeoMeta
from package.mcr import mcr
from package.logger import Timed, rlog
from package.mcr.config import MCRConfig
from package.mcr.data import MCRGeoData
from package.mcr.output import OutputFormat
from package.mcr.steps.bicycle import BicycleStepBuilder
from package.mcr.steps.public_transport import PublicTransportStepBuilder
from package.mcr.steps.walking import WalkingStepBuilder
from package.minute_city import minute_city
from package.osm import osm as osm_lib


def run(
    stops: Annotated[str, typer.Option(help=STOPS_HELP)],
    structs: Annotated[str, typer.Option(help=STRUCTS_HELP)],
    geo_meta_path: Annotated[
        str,
        typer.Option(
            help="Path to the MCR geo meta file.",
        ),
    ],
    start_node_id: Annotated[
        int,
        typer.Option(help="OSM node ID of the start node."),
    ],
    start_time: Annotated[
        str,
        typer.Option(
            help="Start time in the format 'HH:MM:SS'.",
        ),
    ],
    output: Annotated[
        str,
        typer.Option(
            help="Output file path.",
        ),
    ],
    city_id: Annotated[
        str,
        typer.Option(
            help=CITY_ID_HELP,
        ),
    ] = "",
    osm: Annotated[str, typer.Option(help=OSM_HELP)] = "",
    max_transfers: Annotated[
        int,
        typer.Option(
            help="Maximum number of transfers, where a transfer is a public transport ride or a bike ride.",
        ),
    ] = 2,
    disable_paths: Annotated[
        bool,
        typer.Option(
            help="Disable path computation. Speeds up computation, but it will be impossible to retrace the path of a label.",
        ),
    ] = False,
    bicycle_price_function: Annotated[
        str,
        typer.Option(help="Function to be used to calculate the price of a bike ride."),
    ] = "next_bike_no_tariff",
    bicycle_location_path: Annotated[
        str,
        typer.Option(help="Path to the bicycle location file."),
    ] = "",
    output_format: Annotated[
        OutputFormat, typer.Option()
    ] = OutputFormat.CLASS_PICKLE.value,  # type: ignore
    enable_limit: Annotated[
        bool,
        typer.Option(
            help="Discard labels that won't contribute to X-minute city metric (speeds up computation).",
        ),
    ] = False,
):
    validate_flags(
        stops,
        structs,
        city_id,
        osm,
    )

    with Timed.info("Running MCR"):
        geo_meta = GeoMeta.load(geo_meta_path)
        geo_data = MCRGeoData(
            stops,
            structs,
            geo_meta,
            city_id,
            bicycle_location_path=bicycle_location_path,
        )

        with Timed.info("Fetching POI for runtime optimization"):
            pois = minute_city.fetch_pois_for_area(
                geo_meta.boundary, geo_data.original_osm_nodes
            )
            geo_data.add_pois_to_mm_graph(pois)
            geo_data.add_pois_to_walking_graph(pois)

        config = MCRConfig(
            enable_limit=enable_limit,
            disable_paths=disable_paths,
        )

        bicycle_step = BicycleStepBuilder(
            geo_data.mm_graph_cache,
            geo_data.osm_node_to_mm_bicycle_resetted_map,
            geo_data.mm_walking_node_resetted_to_osm_node_map,
            geo_data.bicycle_transfer_osm_node_ids,
            bicycle_price_function,
        )
        public_transport_step = PublicTransportStepBuilder(
            geo_data.structs_dict,
            geo_data.osm_node_to_stop_map,
            geo_data.stop_to_osm_node_map,
        )
        walking_step = WalkingStepBuilder(
            geo_data.walking_graph_cache,
            geo_data.osm_node_to_walking_resetted_map,
            geo_data.walking_resetted_to_osm_node_map,
        )

        initial_steps = [[walking_step]]
        repeating_steps = [
            [bicycle_step, public_transport_step],
            [walking_step],
        ]

        mcr_runner = mcr.MCR(
            initial_steps=initial_steps,
            repeating_steps=repeating_steps,
            config=config,
            output_format=output_format,
        )
        mcr_runner.run(start_node_id, start_time, max_transfers, output)


def validate_flags(
    stops: str,
    structs: str,
    city_id: str,
    osm: str,
):
    if not city_id and not osm:
        raise typer.BadParameter(
            "Either '--city-id' or '--osm' must be provided.",
        )

    if osm and not os.path.isfile(osm):
        raise typer.BadParameter(f"File '{osm}' does not exist.")

    if not os.path.isfile(stops):
        raise typer.BadParameter(f"File '{stops}' does not exist.")

    if not os.path.exists(structs):
        raise typer.BadParameter(f"Structs file {structs} does not exist.")
