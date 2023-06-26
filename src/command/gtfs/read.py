from typing import Annotated
import typer

from package import key
from package.gtfs import read


app = typer.Typer()


@app.command(
    name=key.GTFS_READ_STOPS_COMMAND_NAME,
    help="Read stops from a GTFS feed",
)
def read_stops(
    path: Annotated[
        str,
        typer.Argument(
            help="Path to the GTFS feed to read. Either a the GTFS zip file or a directory containing the unzipped GTFS files",
        ),
    ]
):
    read.print_stops(path)
