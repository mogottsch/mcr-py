from datetime import datetime
from typing_extensions import Annotated
from matplotlib import os
import typer

from package import key, storage
from package.gtfs import clean, catalog, crop
from package.logger import Timed


app = typer.Typer()


@app.command(
    name=key.GTFS_LIST_COMMAND_NAME,
    help="List all available GTFS feeds (https://database.mobilitydata.org/).",
)
def list_command(
    country_code: Annotated[str, typer.Option(help="Country code used to filter")] = "",
    subdivision: Annotated[str, typer.Option(help="Subdivision used to filter")] = "",
    municipality: Annotated[str, typer.Option(help="Municipality used to filter")] = "",
):
    catalog.list_catalog(country_code, subdivision, municipality)


@app.command(name=key.GTFS_DOWNLOAD_COMMAND_NAME, help="Download a GTFS feed by its ID")
def download_command(
    id: Annotated[
        int,
        typer.Argument(
            help=f"ID of the GTFS feed to download, as obtained from the {key.GTFS_LIST_COMMAND_NAME} command",
        ),
    ],
    output: Annotated[
        str,
        typer.Argument(
            help="Path to where the GTFS feed will be downloaded",
        ),
    ],
):
    catalog.download(id, output)


@app.command(
    name=key.GTFS_CROP_COMMAND_NAME,
    help="Crop a downloaded GTFS through a bounding box and/or time range",
)
def crop_command(
    path: Annotated[
        str,
        typer.Argument(
            help="Path to the GTFS feed to crop",
        ),
    ],
    output: Annotated[
        str,
        typer.Argument(
            help="Path to where the cropped GTFS feed will be saved",
        ),
    ],
    lat_min: Annotated[
        float,
        typer.Option(
            help="Minimum latitude of the bounding box",
        ),
    ],
    lat_max: Annotated[
        float,
        typer.Option(
            help="Maximum latitude of the bounding box",
        ),
    ],
    lon_min: Annotated[
        float,
        typer.Option(
            help="Minimum longitude of the bounding box",
        ),
    ],
    lon_max: Annotated[
        float,
        typer.Option(
            help="Maximum longitude of the bounding box",
        ),
    ],
    time_start: Annotated[
        str,
        typer.Option(
            help=f"Start time of the time range, in the format {key.DATE_TIME_FORMAT_HUMAN_READABLE}",
        ),
    ],
    time_end: Annotated[
        str,
        typer.Option(
            help=f"End time of the time range, in the format {key.DATE_TIME_FORMAT_HUMAN_READABLE}",
        ),
    ],
):
    time_start_datetime = datetime.strptime(time_start, key.DATE_TIME_FORMAT)
    time_end_datetime = datetime.strptime(time_end, key.DATE_TIME_FORMAT)

    with Timed.info("Cropping GTFS data"):
        crop.crop(
            path,
            output,
            lat_min=lat_min,
            lat_max=lat_max,
            lon_min=lon_min,
            lon_max=lon_max,
            time_start=time_start_datetime,
            time_end=time_end_datetime,
        )


@app.command(
    name=key.GTFS_CLEAN_COMMAND_NAME,
    help="Clean a GTFS feed",
)
def clean_gtfs(
    gtfs_zip_file: Annotated[str, typer.Argument(help="Path to GTFS zip file")],
    output_dir: Annotated[str, typer.Argument(help="Path to output directory")],
):
    with Timed.info("Cleaning GTFS data"):
        dfs_dict = clean.clean(gtfs_zip_file)

    with Timed.info("Writing GTFS data to output directory"):
        storage.write_dfs_dict(dfs_dict, output_dir)


@app.callback(invoke_without_command=True, no_args_is_help=True)
def main():
    pass
