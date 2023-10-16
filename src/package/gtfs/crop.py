from datetime import datetime
from typing import Tuple

from geopandas import pd
from package import key
from package.geometa import GeoMeta

from package.gtfs import archive
from package.logger import Timed, rlog


def crop(
    path: str,
    output: str,
    geo_meta: GeoMeta,
    time_start: datetime,
    time_end: datetime,
):
    with Timed.info("Reading GTFS data"):
        dfs = archive.read_dfs(path)

    trips_df, stop_times_df, stops_df, calendar_df, routes_df = (
        dfs[key.TRIPS_KEY],
        dfs[key.STOP_TIMES_KEY],
        dfs[key.STOPS_KEY],
        dfs[key.CALENDAR_KEY],
        dfs[key.ROUTES_KEY],
    )

    n_trips, n_stop_times, n_stops = (
        len(trips_df),
        len(stop_times_df),
        len(stops_df),
    )
    rlog.debug(
        f"""
    # of trips: {n_trips}
    # of stop times: {n_stop_times}
    # of stops: {n_stops}
    """
    )

    stops_df = geo_meta.crop_df(stops_df, key.STOP_LAT_KEY, key.STOP_LON_KEY)
    trips_df, stop_times_df = reconcile_trips_and_stop_times_with_stops(
        trips_df, stop_times_df, stops_df
    )
    rlog.debug(
        f"""
        Bounding Box stops remaining: {len(stops_df)}/{n_stops} ({len(stops_df) / n_stops:.2%})
        Bounding Box trips remaining: {len(trips_df)}/{n_trips} ({len(trips_df) / n_trips:.2%})
        """
    )

    n_trips_after_bbox, n_stops_after_bbox = len(trips_df), len(stops_df)
    if n_trips_after_bbox == 0 or n_stops_after_bbox == 0:
        raise ValueError(
            f"Bounding box is too small, no trips or stops remain: {n_trips_after_bbox} trips, {n_stops_after_bbox} stops"
        )

    trips_df, calendar_df = crop_trips(trips_df, calendar_df, time_start, time_end)
    stop_times_df = reconcile_stop_times_with_trips(stop_times_df, trips_df)
    stops_df = reconcile_stops_with_stop_times(stops_df, stop_times_df)

    rlog.debug(
        f"""
        Time range trips remaining: {len(trips_df)}/{n_trips_after_bbox} ({len(trips_df) / n_trips_after_bbox:.2%})
        Time range stops remaining: {len(stops_df)}/{n_stops_after_bbox} ({len(stops_df) / n_stops_after_bbox:.2%})
        Time range stop times remaining: {len(stop_times_df)}/{n_stop_times} ({len(stop_times_df) / n_stop_times:.2%})
        """
    )

    rlog.info(
        f"""\
        Crop results:
        # of trips: {len(trips_df)} ({len(trips_df) / n_trips:.2%})
        # of stop times: {len(stop_times_df)} ({len(stop_times_df) / n_stop_times:.2%})
        # of stops: {len(stops_df)} ({len(stops_df) / n_stops:.2%})"""
    )

    archive.write_dfs(
        {
            key.TRIPS_KEY: trips_df,
            key.STOP_TIMES_KEY: stop_times_df,
            key.STOPS_KEY: stops_df,
            key.CALENDAR_KEY: calendar_df,
            key.ROUTES_KEY: routes_df,  # todo this is not being cropped, but is small anyways
        },
        output,
    )


def reconcile_trips_and_stop_times_with_stops(
    trips_df: pd.DataFrame,
    stop_times_df: pd.DataFrame,
    stops_df: pd.DataFrame,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Crop trips and stop times to the given stops.
    """
    stop_ids = stops_df[key.STOP_ID_KEY].unique()
    stop_times_df = stop_times_df[stop_times_df[key.STOP_ID_KEY].isin(stop_ids)]  # type: ignore
    # only keep trips that have at least two entries in stop_times_df
    stop_times_df = stop_times_df[stop_times_df[key.TRIP_ID_KEY].duplicated(keep=False)]  # type: ignore
    trips_df = trips_df[trips_df[key.TRIP_ID_KEY].isin(stop_times_df[key.TRIP_ID_KEY])]  # type: ignore

    return trips_df, stop_times_df


def crop_trips(
    trips_df: pd.DataFrame,
    calendar_df: pd.DataFrame,
    time_start: datetime,
    time_end: datetime,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Crop trips to those that occur within the given time range.
    """
    parsed_calendar_df = calendar_df.copy()
    parsed_calendar_df[key.CALENDAR_START_DATE_KEY] = pd.to_datetime(
        parsed_calendar_df[key.CALENDAR_START_DATE_KEY],
        format=key.CALENDAR_DATE_TIME_FORMAT,
    )
    parsed_calendar_df[key.CALENDAR_END_DATE_KEY] = pd.to_datetime(
        parsed_calendar_df[key.CALENDAR_END_DATE_KEY],
        format=key.CALENDAR_DATE_TIME_FORMAT,
    )

    time_ranges_touch = (
        parsed_calendar_df[key.CALENDAR_START_DATE_KEY] <= time_end
    ) & (parsed_calendar_df[key.CALENDAR_END_DATE_KEY] >= time_start)

    calendar_df = calendar_df[time_ranges_touch]  # type: ignore

    service_ids = calendar_df[key.SERVICE_ID_KEY].unique()
    trips_df = trips_df[trips_df[key.SERVICE_ID_KEY].isin(service_ids)]  # type: ignore

    return trips_df, calendar_df


def reconcile_stop_times_with_trips(
    stop_times_df: pd.DataFrame,
    trips_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Crop stop times to those that occur within the given trips.
    """
    trip_ids = trips_df[key.TRIP_ID_KEY].unique()
    stop_times_df = stop_times_df[stop_times_df[key.TRIP_ID_KEY].isin(trip_ids)]  # type: ignore

    return stop_times_df


def reconcile_stops_with_stop_times(
    stops_df: pd.DataFrame,
    stop_times_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Crop stops to those that occur within the given stop times.
    """
    stop_ids = stop_times_df[key.STOP_ID_KEY].unique()
    stops_df = stops_df[stops_df[key.STOP_ID_KEY].isin(stop_ids)]  # type: ignore

    return stops_df
