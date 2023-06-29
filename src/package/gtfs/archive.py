import zipfile

import pandas as pd
from package.gtfs import dtypes


from package.key import (
    STOP_TIMES_KEY,
    TRIPS_KEY,
    STOPS_KEY,
)
from package.logger import llog


def get_gtfs_filename(name: str) -> str:
    return f"{name}.txt"


STOPS_FILE = get_gtfs_filename(STOPS_KEY)
TRIPS_FILE = get_gtfs_filename(TRIPS_KEY)
STOP_TIMES_FILE = get_gtfs_filename(STOP_TIMES_KEY)
CALENDAR_FILE = get_gtfs_filename("calendar")
ROUTES_FILE = get_gtfs_filename("routes")


EXPECTED_FILES = [
    STOPS_FILE,
    TRIPS_FILE,
    STOP_TIMES_FILE,
    CALENDAR_FILE,
    ROUTES_FILE,
]


def read_dfs(gtfs_zip_path: str) -> dict[str, pd.DataFrame]:
    """
    Reads GTFS zip file and returns a dictionary of dataframes.
    """
    dfs = {}

    with zipfile.ZipFile(gtfs_zip_path, "r") as zip_ref:
        contained = zip_ref.namelist()

        for expected_file in EXPECTED_FILES:
            if expected_file not in contained:
                raise Exception(f"Expected file {expected_file} not in zip file")

        for file in EXPECTED_FILES:
            df = read_file(zip_ref, file)
            name = file.split(".")[0]
            dfs[name] = df

    return dfs


def read_file(zip_ref: zipfile.ZipFile, file: str) -> pd.DataFrame:
    with zip_ref.open(file) as f:
        llog.debug(f"Reading {file}")
        df = pd.read_csv(f, dtype=dtypes.GTFS_DTYPES)  # type: ignore
        return df


def write_dfs(dfs: dict[str, pd.DataFrame], output: str):
    """
    Writes a dictionary of dataframes to a GTFS zip file.
    """
    with zipfile.ZipFile(output, "w") as zip_ref:
        for name, df in dfs.items():
            file = get_gtfs_filename(name)
            write_file(zip_ref, file, df)


def write_file(zip_ref: zipfile.ZipFile, file: str, df: pd.DataFrame):
    with zip_ref.open(file, "w") as f:
        df.to_csv(f, index=False)
