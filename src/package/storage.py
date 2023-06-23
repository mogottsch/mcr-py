import pandas as pd
import geopandas as gpd
import os


def write_dfs_dict(dfs_dict: dict[str, pd.DataFrame], output_path: str):
    os.makedirs(output_path, exist_ok=True)

    for name, df in dfs_dict.items():
        filename = get_df_filename_for_name(name)

        df.to_csv(os.path.join(output_path, filename), index=False)


def get_df_filename_for_name(name: str) -> str:
    return f"{name}.csv"


def read_gdf(path: str) -> gpd.GeoDataFrame:
    return gpd.read_file(
        path, GEOM_POSSIBLE_NAMES="geometry", KEEP_GEOM_COLUMNS="NO"
    ).set_crs("EPSG:4326")
