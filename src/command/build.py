import os
from typing import Annotated
import typer
from package import storage

from package.key import (
    TRIPS_KEY,
    STOP_TIMES_KEY,
)
from package.build import build_structures as build_structures_direct
from package.logger import Timed


def build_structures(
    clean_gtfs_dir: Annotated[str, typer.Argument(help="Path to clean GTFS directory")],
    output_file: Annotated[str, typer.Argument(help="Path to output file")],
):
    trips_df = storage.read_df(
        os.path.join(clean_gtfs_dir, storage.get_df_filename_for_name(TRIPS_KEY))
    )
    stop_times_df = storage.read_df(
        os.path.join(clean_gtfs_dir, storage.get_df_filename_for_name(STOP_TIMES_KEY))
    )

    with Timed.info("Building structures"):
        data = build_structures_direct(trips_df, stop_times_df)

    storage.write_any_dict(data, output_file)
