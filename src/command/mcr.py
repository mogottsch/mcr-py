from typing_extensions import Annotated

import typer
from pyrosm.data import os

from command.footpaths import CITY_ID_HELP, OSM_HELP, STOPS_HELP
from command.raptor import STRUCTS_HELP
from package import storage
from package.mcr import mcr
from package.logger import Timed, rlog
from package.mcr.data import MCRGeoData
from package.mcr.output import OutputFormat
from package.minute_city import minute_city
from package.osm import osm as osm_lib


def run(
    stops: Annotated[str, typer.Option(help=STOPS_HELP)],
    structs: Annotated[str, typer.Option(help=STRUCTS_HELP)],
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
        typer.Option(),
    ] = "next_bike_no_tariff",
    output_format: Annotated[
        OutputFormat, typer.Option()
    ] = OutputFormat.CLASS_PICKLE.value,  # type: ignore
):
    validate_flags(
        stops,
        structs,
        city_id,
        osm,
    )

    with Timed.info("Running MCR"):
        mcr_geo_data = MCRGeoData(stops, structs, city_id)
        with Timed.info("Reading stops"):
            other_stops_df = storage.read_gdf(stops)

        osm_path = osm_lib.get_osm_path_from_city_id(city_id)
        if not os.path.exists(osm_path) and city_id:
            rlog.info("Downloading OSM data")
            osm_lib.download_city(city_id, osm_path)
        else:
            rlog.info("Using existing OSM data")

        osm_reader = osm_lib.new_osm_reader(osm_path)

        with Timed.info("Getting OSM graph"):
            nodes, _ = osm_lib.get_graph_for_city_cropped_to_stops(
                osm_reader, other_stops_df
            )
        pois = minute_city.fetch_pois_for_area(nodes.unary_union.convex_hull, nodes)
        mcr_geo_data.add_pois_to_mm_graph(pois)
        mcr_runner = mcr.MCR(
            mcr_geo_data,
            disable_paths=disable_paths,
            bicycle_price_function=bicycle_price_function,
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
