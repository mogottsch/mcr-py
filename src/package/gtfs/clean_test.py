import pytest
import pandas as pd
from package.gtfs.clean import (
    add_first_stop_info,
    add_unique_route_ids,
    create_paths_df,
    remove_unused_stops,
    split_routes_by_direction,
)


def test_split_routes_by_direction(trips_df: pd.DataFrame):
    split_routes_by_direction(trips_df)

    expected_route_ids = pd.Series(
        ["route1_0", "route1_0", "route1_1"], name="route_id"
    )
    pd.testing.assert_series_equal(trips_df["route_id"], expected_route_ids)


def test_create_paths_df(trips_df: pd.DataFrame, stop_times_df: pd.DataFrame):
    paths_df = create_paths_df(trips_df, stop_times_df)
    expected_paths = pd.Series(
        [
            "['stop1', 'stop2', 'stop3']",
            "['stop1', 'stop4', 'stop3']",
            "['stop3', 'stop2', 'stop1']",
        ],
        name="path",
    )
    pd.testing.assert_series_equal(paths_df["path"], expected_paths)


def test_add_unique_route_ids(paths_df: pd.DataFrame):
    paths_df = add_unique_route_ids(paths_df)
    expected_route_ids = pd.Series(
        ["route1_0_A", "route1_0_B", "route1_1_A"], name="new_route_id"
    )
    pd.testing.assert_series_equal(paths_df["new_route_id"], expected_route_ids)


def test_add_first_stop_info(trips_df: pd.DataFrame, stop_times_df: pd.DataFrame):
    trips_df = add_first_stop_info(trips_df, stop_times_df)
    expected_first_stop_ids = pd.Series(
        ["stop1", "stop1", "stop3"], name="first_stop_id"
    )
    expected_first_stop_departure_times = pd.Series(
        ["00:00:00", "01:00:00", "02:00:00"], name="trip_departure_time"
    )
    pd.testing.assert_series_equal(trips_df["first_stop_id"], expected_first_stop_ids)
    pd.testing.assert_series_equal(
        trips_df["trip_departure_time"], expected_first_stop_departure_times
    )


def test_remove_unused_stops(stop_times_df: pd.DataFrame, stops_df: pd.DataFrame):
    stops_df = remove_unused_stops(stop_times_df, stops_df)
    expected_stops = pd.Series(["stop1", "stop2", "stop3", "stop4"], name="stop_id")
    pd.testing.assert_series_equal(stops_df["stop_id"], expected_stops)
