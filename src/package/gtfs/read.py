from rich.table import Table
from rich.console import Console
from pyrosm.data import os
import pandas as pd

from package import key, storage
from package.gtfs import archive
from package.logger import Timed, llog


def print_stops(path: str):
    with Timed.info("Reading stops"):
        stops_df = get_stops_df(path)
    print_dataframe(stops_df)


def get_stops_df(path: str):
    if path.endswith(".zip"):
        llog.debug("Reading stops from zip file")
        dfs = archive.read_dfs(path)
        return dfs[key.STOPS_KEY]

    if not os.path.isdir(path):
        raise ValueError("Path is neither a zip file nor a directory")

    llog.debug("Reading stops from directory")

    return storage.read_df(
        os.path.join(path, storage.get_df_filename_for_name(key.STOPS_KEY))
    )


def print_dataframe(
    df: pd.DataFrame,
):
    table = Table(title="Stops")
    table.add_column("Stop ID")
    table.add_column("Stop Name")
    table.add_column("Google maps link")

    for _, row in df.iterrows():
        table.add_row(
            format_value(row[key.STOP_ID_KEY]),  # type: ignore
            format_value(row[key.STOP_NAME_KEY]),  # type: ignore
            format_value(f"https://maps.google.com/?q={row[key.STOP_LAT_KEY]},{row[key.STOP_LON_KEY]}"),  # type: ignore
        )

    console = Console()
    console.print(table)


def format_value(value):
    return str(value)
