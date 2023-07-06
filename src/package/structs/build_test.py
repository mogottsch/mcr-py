import pandas as pd

from package.structs.build import (
    create_id_sets,
    create_idx_by_stop_by_route,
    create_routes_by_stop,
    create_stop_times_by_trip,
    create_times_by_stop_by_trip,
    create_trip_ids_by_route_sorted_by_departure,
)
from package import strtime
from package.structs.build import create_stops_by_route_ordered


def test_create_stop_times_by_trip(stop_times_df: pd.DataFrame):
    expected_stop_times_by_trip = {
        "trip1": [
            {
                "departure_time": "00:00:00",
                "arrival_time": "00:00:00",
                "stop_id": "stop1",
                "stop_sequence": 1,
            },
            {
                "departure_time": "00:10:00",
                "arrival_time": "00:10:00",
                "stop_id": "stop2",
                "stop_sequence": 2,
            },
            {
                "departure_time": "00:20:00",
                "arrival_time": "00:20:00",
                "stop_id": "stop3",
                "stop_sequence": 3,
            },
        ],
        "trip2": [
            {
                "departure_time": "01:00:00",
                "arrival_time": "01:00:00",
                "stop_id": "stop1",
                "stop_sequence": 1,
            },
            {
                "departure_time": "01:10:00",
                "arrival_time": "01:10:00",
                "stop_id": "stop4",
                "stop_sequence": 2,
            },
            {
                "departure_time": "01:20:00",
                "arrival_time": "01:20:00",
                "stop_id": "stop3",
                "stop_sequence": 3,
            },
        ],
        "trip3": [
            {
                "departure_time": "02:00:00",
                "arrival_time": "02:00:00",
                "stop_id": "stop3",
                "stop_sequence": 1,
            },
            {
                "departure_time": "02:10:00",
                "arrival_time": "02:10:00",
                "stop_id": "stop2",
                "stop_sequence": 2,
            },
            {
                "departure_time": "02:20:00",
                "arrival_time": "02:20:00",
                "stop_id": "stop1",
                "stop_sequence": 3,
            },
        ],
    }

    stop_times_by_trip = create_stop_times_by_trip(stop_times_df)
    assert stop_times_by_trip == expected_stop_times_by_trip


def test_create_trip_ids_by_route_sorted_by_departure(cleaned_trips_df: pd.DataFrame):
    expected_trip_ids_by_route = {
        "route1_0_A": ["trip1"],
        "route1_0_B": ["trip2"],
        "route1_1_A": ["trip3"],
    }

    trip_ids_by_route = create_trip_ids_by_route_sorted_by_departure(cleaned_trips_df)
    assert trip_ids_by_route == expected_trip_ids_by_route


def test_create_stops_by_route(
    trip_ids_by_route: dict[str, list[str]],
    stop_times_by_trip: dict[str, list[dict[str, str]]],
):
    stops_by_route = create_stops_by_route_ordered(
        trip_ids_by_route, stop_times_by_trip
    )

    expected_stops_by_route = {
        "route1_0_A": ["stop1", "stop2", "stop3"],
        "route1_0_B": ["stop1", "stop4", "stop3"],
        "route1_1_A": ["stop3", "stop2", "stop1"],
    }

    assert stops_by_route == expected_stops_by_route


def test_create_routes_by_stop(stops_by_route: dict[str, list[str]]):
    routes_by_stop = create_routes_by_stop(stops_by_route)

    expected_routes_by_stop = {
        "stop1": set(["route1_0_A", "route1_0_B", "route1_1_A"]),
        "stop2": set(["route1_0_A", "route1_1_A"]),
        "stop3": set(["route1_0_A", "route1_0_B", "route1_1_A"]),
        "stop4": set(["route1_0_B"]),
    }

    assert routes_by_stop == expected_routes_by_stop


def test_create_id_sets(
    cleaned_trips_df: pd.DataFrame, routes_by_stop: dict[str, set[str]]
):
    stop_id_set, route_id_set, trip_id_set = create_id_sets(
        cleaned_trips_df, routes_by_stop
    )

    stop_id_set_expected = set(["stop1", "stop2", "stop3", "stop4"])
    route_id_set_expected = set(["route1_0_A", "route1_0_B", "route1_1_A"])
    trip_id_set_expected = set(["trip1", "trip2", "trip3"])

    assert stop_id_set == stop_id_set_expected
    assert route_id_set == route_id_set_expected
    assert trip_id_set == trip_id_set_expected


def test_create_idx_by_stop_by_route(
    stops_by_route: dict[str, list[str]],
):
    idx_by_stop_by_route = create_idx_by_stop_by_route(stops_by_route)

    expected_idx_by_stop_by_route = {
        "route1_0_A": {
            "stop1": 0,
            "stop2": 1,
            "stop3": 2,
        },
        "route1_0_B": {
            "stop1": 0,
            "stop4": 1,
            "stop3": 2,
        },
        "route1_1_A": {
            "stop3": 0,
            "stop2": 1,
            "stop1": 2,
        },
    }

    assert idx_by_stop_by_route == expected_idx_by_stop_by_route


def test_create_times_by_stop_by_trip(
    stop_times_by_trip: dict[str, list[dict[str, str]]]
):
    times_by_stop_by_trip = create_times_by_stop_by_trip(stop_times_by_trip)

    expected_times_by_stop_by_trip = {
        "trip1": {
            "stop1": (
                strtime.str_time_to_seconds("00:00:00"),
                strtime.str_time_to_seconds("00:00:00"),
            ),
            "stop2": (
                strtime.str_time_to_seconds("00:10:00"),
                strtime.str_time_to_seconds("00:10:00"),
            ),
            "stop3": (
                strtime.str_time_to_seconds("00:20:00"),
                strtime.str_time_to_seconds("00:20:00"),
            ),
        },
        "trip2": {
            "stop1": (
                strtime.str_time_to_seconds("01:00:00"),
                strtime.str_time_to_seconds("01:00:00"),
            ),
            "stop4": (
                strtime.str_time_to_seconds("01:10:00"),
                strtime.str_time_to_seconds("01:10:00"),
            ),
            "stop3": (
                strtime.str_time_to_seconds("01:20:00"),
                strtime.str_time_to_seconds("01:20:00"),
            ),
        },
        "trip3": {
            "stop3": (
                strtime.str_time_to_seconds("02:00:00"),
                strtime.str_time_to_seconds("02:00:00"),
            ),
            "stop2": (
                strtime.str_time_to_seconds("02:10:00"),
                strtime.str_time_to_seconds("02:10:00"),
            ),
            "stop1": (
                strtime.str_time_to_seconds("02:20:00"),
                strtime.str_time_to_seconds("02:20:00"),
            ),
        },
    }

    assert times_by_stop_by_trip == expected_times_by_stop_by_trip


