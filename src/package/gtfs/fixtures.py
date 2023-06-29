import pytest
import pandas as pd
from package.gtfs.clean import (
    add_first_stop_info,
    create_paths_df,
    split_routes_by_direction,
    split_routes,
)

ROUTE1_ID = "route1"
ROUTES = {
    ROUTE1_ID: {
        "trip1": {
            "stop_times": {
                "stop1": {
                    "stop_sequence": 1,
                    "departure_time": "00:00:00",
                    "arrival_time": "00:00:00",
                },
                "stop2": {
                    "stop_sequence": 2,
                    "departure_time": "00:10:00",
                    "arrival_time": "00:10:00",
                },
                "stop3": {
                    "stop_sequence": 3,
                    "departure_time": "00:20:00",
                    "arrival_time": "00:20:00",
                },
            },
            "direction": 0,
        },
        "trip2": {
            "stop_times": {
                "stop1": {
                    "stop_sequence": 1,
                    "departure_time": "01:00:00",
                    "arrival_time": "01:00:00",
                },
                "stop4": {
                    "stop_sequence": 2,
                    "departure_time": "01:10:00",
                    "arrival_time": "01:10:00",
                },
                "stop3": {
                    "stop_sequence": 3,
                    "departure_time": "01:20:00",
                    "arrival_time": "01:20:00",
                },
            },
            "direction": 0,
        },
        "trip3": {
            "stop_times": {
                "stop3": {
                    "stop_sequence": 1,
                    "departure_time": "02:00:00",
                    "arrival_time": "02:00:00",
                },
                "stop2": {
                    "stop_sequence": 2,
                    "departure_time": "02:10:00",
                    "arrival_time": "02:10:00",
                },
                "stop1": {
                    "stop_sequence": 3,
                    "departure_time": "02:20:00",
                    "arrival_time": "02:20:00",
                },
            },
            "direction": 1,
        },
    }
}


@pytest.fixture
def trips_df() -> pd.DataFrame:
    # route_id, trip_id, direction_id
    trips = ROUTES[ROUTE1_ID]
    return pd.DataFrame(
        [[ROUTE1_ID, trip_id, trips[trip_id]["direction"]] for trip_id in trips],
        columns=["route_id", "trip_id", "direction_id"],
    )


@pytest.fixture
def stop_times_df() -> pd.DataFrame:
    # trip_id, departure_time, arrival_time, stop_id, stop_sequence
    trips = ROUTES[ROUTE1_ID]
    stop_times = []
    for trip_id in trips:
        stop_times.extend(
            [
                [
                    trip_id,
                    trips[trip_id]["stop_times"][stop_id]["departure_time"],
                    trips[trip_id]["stop_times"][stop_id]["arrival_time"],
                    stop_id,
                    trips[trip_id]["stop_times"][stop_id]["stop_sequence"],
                ]
                for stop_id in trips[trip_id]["stop_times"]
            ]
        )
    return pd.DataFrame(
        stop_times,
        columns=[
            "trip_id",
            "departure_time",
            "arrival_time",
            "stop_id",
            "stop_sequence",
        ],
    )


@pytest.fixture
def paths_df(trips_df: pd.DataFrame, stop_times_df: pd.DataFrame) -> pd.DataFrame:
    split_routes_by_direction(trips_df)
    return create_paths_df(trips_df, stop_times_df)


@pytest.fixture
def stops_df() -> pd.DataFrame:
    # stop_id
    return pd.DataFrame(
        [["stop1"], ["stop2"], ["stop3"], ["stop4"], ["stop5"]], columns=["stop_id"]
    )


@pytest.fixture()
def cleaned_trips_df(
    trips_df: pd.DataFrame, stop_times_df: pd.DataFrame
) -> pd.DataFrame:
    trips_df = split_routes(trips_df, stop_times_df)
    trips_df = add_first_stop_info(trips_df, stop_times_df)
    return trips_df
