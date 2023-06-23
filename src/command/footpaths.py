from logging import INFO
from typing_extensions import Annotated
import typer

from package import storage
from package.footpaths import generate as direct_generate
from package.logger import Timed
from package.key import CLEAN_GTFS_COMMAND_NAME, STOPS_KEY

CLEAN_STOPS_FILENAME = storage.get_df_filename_for_name(STOPS_KEY)

CITY_ID_HELP = """
City ID used for pyrosm, see pyrosm "Available datasets" for more information. 
The area of the dataset associated with the city ID should be at least as \
large as the area (convex hull) of the stops.
Required if '--osm' is not provided.
"""

OSM_HELP = """
OSM pbf file.
The area of the dataset should be at least as large as the area (convex hull) \
of the stops.
Required if '--city-id' is not provided.
"""

STOPS_HELP = f"""
A path that should point to either {CLEAN_STOPS_FILENAME} or a directory \
containing {CLEAN_STOPS_FILENAME}, as given by the output of the \
{CLEAN_GTFS_COMMAND_NAME} command.
"""


def generate(
    output: Annotated[str, typer.Option(help="Output file in pickle format.")],
    stops: Annotated[str, typer.Option(help=STOPS_HELP)],
    avg_walking_speed: Annotated[
        float,
        typer.Option(
            help="Average walking speed in meters per second.",
        ),
    ] = 1.4,
    max_walking_duration: Annotated[
        int,
        typer.Option(
            help="Maximum walking duration in seconds.",
        ),
    ] = 10
    * 60,
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
        avg_walking_speed,
        max_walking_duration,
        output,
    )
    direct_generate(
        city_id,
        osm,
        stops,
        avg_walking_speed,
        max_walking_duration,
    )


def validate_flags(
    city_id: str,
    osm: str,
    stops: str,
    avg_walking_speed: float,
    max_walking_duration: int,
    output: str,
):
    if not city_id and not osm:
        raise typer.BadParameter(
            "Either '--city-id' or '--osm' must be provided.",
        )
