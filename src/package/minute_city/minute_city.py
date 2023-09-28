from shapely.geometry import Polygon
from tqdm.auto import tqdm
from package.osm import osm
import geopandas as gpd
from package.overpass import attributes, query
import pandas as pd
from functools import partial
from concurrent.futures import ProcessPoolExecutor
import multiprocessing
from package.logger import rlog, Timed

from package.minute_city import profile


def fetch_pois_for_area(
    area_of_interest: Polygon, nodes: gpd.GeoDataFrame
) -> gpd.GeoDataFrame:
    """
    Fetches POIs for the given area of interest and assigns the nearest osm node id to each POI.

    Args:
        area_of_interest (Polygon): The area of interest to fetch POIs for.
        nodes (gpd.GeoDataFrame): The OSM nodes to use for assigning nearest osm node ids.

    Returns:
        gpd.GeoDataFrame: The POIs for the given area of interest.
    """
    bbox: tuple[float, float, float, float] = area_of_interest.bounds  # type: ignore

    queries = [
        (name, query.build(attr, bbox))
        for name, attr in attributes.X_MINUTE_CITY_QUERIES
    ]
    pois = query.fetch_and_merge_queries_async(queries, area_of_interest)
    pois: gpd.GeoDataFrame = osm.add_nearest_osm_node_id(pois, nodes)  # type: ignore

    return pois


def add_pois_to_labels(labels: pd.DataFrame, pois: gpd.GeoDataFrame) -> pd.DataFrame:
    poi_types = list(pois["type"].unique())
    for t in poi_types:
        pois[t] = (pois["type"] == t).astype(int)

    labels = labels.merge(
        pois[["nearest_osm_node_id"] + poi_types],
        left_on="target_id_osm",
        right_on="nearest_osm_node_id",
    )

    return labels


def get_profiles_df(labels_with_pois: gpd.GeoDataFrame, types: list[str]):
    """
    Calculates the profiles for the given labels.
    """
    with Timed.info("Grouping labels"):
        grouped = labels_with_pois.groupby("start_id_hex")
        n_groups = len(grouped)

    partial_worker = partial(profile.profile_calculation_worker, types)

    profiles = {}
    with (
        Timed.info("Calculating profiles"),
        ProcessPoolExecutor(max_workers=multiprocessing.cpu_count() - 2) as executor,
    ):
        pbar = tqdm(total=n_groups)
        for result in executor.map(partial_worker, grouped):
            pbar.update(1)
            if result is not None:
                name, prof = result
                profiles[name] = prof
        pbar.close()

    # df

    with Timed.info("Creating profiles dataframe"):
        start_time: int = labels_with_pois["time"].min()  # type: ignore
        profiles_df = profile.build_profiles_df(profiles, start_time)

        # tuning
        profiles_df = profile.add_any_column_is_different_column(profiles_df)
        profiles_df = profile.add_required_cost_for_optimum_column(profiles_df)

    return profiles_df
