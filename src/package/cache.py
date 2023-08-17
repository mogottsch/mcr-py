import os
import hashlib
import tempfile

import pandas as pd
import geopandas as gpd
from pandas.util import hash_pandas_object

from package import key

tempdir = os.path.join(tempfile.gettempdir(), key.TMP_DIR_NAME)


def hash_gdf(gdf: gpd.GeoDataFrame) -> int:
    return int(
        hashlib.sha256(hash_pandas_object(gdf, index=True).values).hexdigest(),  # type: ignore
        16,
    )


def hash_str(s: str) -> int:
    return int(hashlib.sha256(s.encode("utf-8")).hexdigest(), 16)


def combine_hashes(hashes: list[int]) -> int:
    return int(
        hashlib.sha256("".join([str(h) for h in hashes]).encode("utf-8"))
        .digest()
        .hex(),
        16,
    )


def cache_gdf(df: pd.DataFrame, hash: int, identifier: str):
    if not os.path.exists(tempdir):
        os.mkdir(tempdir)
    path = os.path.join(tempdir, f"{identifier}_{hash}")
    df.to_feather(path)
    # df.to_file(path)


def read_gdf(hash: int, identifier: str) -> gpd.GeoDataFrame:
    path = os.path.join(tempdir, f"{identifier}_{hash}")
    # return gpd.read_file(path)
    return gpd.read_feather(path)


def cache_entry_exists(hash: int, identifier: str) -> bool:
    path = os.path.join(tempdir, f"{identifier}_{hash}")
    return os.path.exists(path)
