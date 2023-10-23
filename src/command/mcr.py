from typing_extensions import Annotated

import typer
from pyrosm.data import os

from command.footpaths import CITY_ID_HELP, OSM_HELP, STOPS_HELP
from command.raptor import STRUCTS_HELP
from command.step_config import (
    ALL_CONFIGS,
    BICYCLE_AND_PUBLIC_TRANSPORT_CONFIG,
    BICYCLE_CONFIG,
    CAR_CONFIG,
    PUBLIC_TRANSPORT_CONFIG,
    WALKING_CONFIG,
    get_bicycle_only_config,
    get_bicycle_public_transport_config,
    get_car_only_config,
    get_public_transport_only_config,
    get_walking_only_config,
)
from package.mcr import mcr
from package.logger import Timed
from package.mcr.config import MCRConfig
from package.mcr.output import OutputFormat


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
    step_config: Annotated[
        str,
        typer.Option(
            help=f"Step config preconfiguration id. Possible values: {', '.join(ALL_CONFIGS)}.",
        ),
    ] = ALL_CONFIGS[0],
):
    validate_flags(
        stops,
        structs,
        city_id,
        osm,
    )

    with Timed.info("Running MCR"):
        if step_config == BICYCLE_AND_PUBLIC_TRANSPORT_CONFIG:
            initial_steps, repeating_steps = get_bicycle_public_transport_config(
                geo_meta_path,
                city_id,
                bicycle_price_function,
                bicycle_location_path,
                structs,
                stops,
            )
        elif step_config == CAR_CONFIG:
            initial_steps, repeating_steps = get_car_only_config(
                geo_meta_path,
                city_id,
            )
        elif step_config == BICYCLE_CONFIG:
            initial_steps, repeating_steps = get_bicycle_only_config(
                geo_meta_path,
                city_id,
                bicycle_price_function,
                bicycle_location_path,
            )
        elif step_config == PUBLIC_TRANSPORT_CONFIG:
            initial_steps, repeating_steps = get_public_transport_only_config(
                geo_meta_path=geo_meta_path,
                city_id=city_id,
                structs_path=structs,
                stops_path=stops,
            )
        elif step_config == WALKING_CONFIG:
            initial_steps, repeating_steps = get_walking_only_config(
                geo_meta_path=geo_meta_path,
                city_id=city_id,
            )

        else:
            raise typer.BadParameter(
                f"Invalid step config '{step_config}'. Possible values: {', '.join(ALL_CONFIGS)}.",
            )

        config = MCRConfig(
            enable_limit=enable_limit,
            disable_paths=disable_paths,
        )

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
