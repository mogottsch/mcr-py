from typing_extensions import Any
import requests
import tempfile
import pandas as pd
import geopandas as gpd
import os
import pickle

from package.gtfs import dtypes
from package.logger import llog


def write_dfs_dict(dfs_dict: dict[str, pd.DataFrame], output_path: str):
    os.makedirs(output_path, exist_ok=True)

    for name, df in dfs_dict.items():
        filename = get_df_filename_for_name(name)

        df.to_csv(os.path.join(output_path, filename), index=False)


def write_df(df: pd.DataFrame, output_path: str):
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    df.to_csv(output_path, index=False)


def get_df_filename_for_name(name: str) -> str:
    return f"{name}.csv"


def read_df(path: str) -> pd.DataFrame:
    return pd.read_csv(path, dtype=dtypes.GTFS_DTYPES)  # type: ignore


def read_gdf(path: str) -> gpd.GeoDataFrame:
    return gpd.read_file(
        path,
        GEOM_POSSIBLE_NAMES="geometry",
        KEEP_GEOM_COLUMNS="NO",
        dtype=dtypes.GTFS_DTYPES,  # not working
    ).set_crs("EPSG:4326")


def write_any_dict(data: dict[str, Any], output_path: str):
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    with open(output_path, "wb") as f:
        pickle.dump(data, f)


def read_any_dict(path: str) -> dict[str, Any]:
    with open(path, "rb") as f:
        return pickle.load(f)


def get_tmp_path(*paths: str) -> str:
    return os.path.join(tempfile.gettempdir(), *paths)


def download_file(url: str, path: str):
    os.makedirs(os.path.dirname(path), exist_ok=True)

    with open(path, "wb") as f:
        f.write(requests.get(url).content)
