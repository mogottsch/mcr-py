from typing_extensions import Annotated

import typer
from pyrosm.data import os

from command.footpaths import CITY_ID_HELP, OSM_HELP, STOPS_HELP
from package.mcr import mcr
from package.logger import Timed


def run(
    stops: Annotated[str, typer.Option(help=STOPS_HELP)],
    city_id: Annotated[
        str,
        typer.Option(
            help=CITY_ID_HELP,
        ),
    ] = "",
    osm: Annotated[str, typer.Option(help=OSM_HELP)] = "",
):
    validate_flags(
        city_id,
        osm,
        stops,
    )

    with Timed.info("Running MCR"):
        mcr.run(
            stops,
            city_id,
            osm,
        )


def validate_flags(
    city_id: str,
    osm: str,
    stops: str,
):
    if not city_id and not osm:
        raise typer.BadParameter(
            "Either '--city-id' or '--osm' must be provided.",
        )

    if osm and not os.path.isfile(osm):
        raise typer.BadParameter(f"File '{osm}' does not exist.")

    if not os.path.isfile(stops):
        raise typer.BadParameter(f"File '{stops}' does not exist.")
