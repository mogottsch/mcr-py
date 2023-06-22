import os
import pandas as pd
from typing import Annotated, Any
import typer
import pickle

from package.key import (
    CLEAN_TRIPS_FILENAME,
    CLEAN_STOP_TIMES_FILENAME,
)
from package.build import build_structures as build_structures_direct


def build_structures(
    clean_gtfs_dir: Annotated[str, typer.Argument(help="Path to clean GTFS directory")],
    output_file: Annotated[str, typer.Argument(help="Path to output file")],
):
    trips_df = pd.read_csv(os.path.join(clean_gtfs_dir, CLEAN_TRIPS_FILENAME))
    stop_times_df = pd.read_csv(os.path.join(clean_gtfs_dir, CLEAN_STOP_TIMES_FILENAME))

    data = build_structures_direct(trips_df, stop_times_df)

    save_as_pickle(data, output_file)


def save_as_pickle(data: dict[str, Any], output_file: str):
    with open(output_file, "wb") as f:
        pickle.dump(data, f)
