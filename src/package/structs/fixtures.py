import pytest
import pandas as pd

from package.structs.build import (
    create_id_sets,
    create_idx_by_stop_by_route,
    create_routes_by_stop,
    create_stop_times_by_trip,
    create_stops_by_route_ordered,
    create_times_by_stop_by_trip,
    create_trip_ids_by_route_sorted_by_departure,
)


@pytest.fixture
def trip_ids_by_route(cleaned_trips_df: pd.DataFrame) -> dict[str, list[str]]:
    return create_trip_ids_by_route_sorted_by_departure(cleaned_trips_df)


@pytest.fixture
def stop_times_by_trip(stop_times_df: pd.DataFrame) -> dict[str, list[dict[str, str]]]:
    return create_stop_times_by_trip(stop_times_df)


@pytest.fixture
def stops_by_route(
    trip_ids_by_route: dict[str, list[str]],
    stop_times_by_trip: dict[str, list[dict[str, str]]],
) -> dict[str, list[str]]:
    return create_stops_by_route_ordered(trip_ids_by_route, stop_times_by_trip)


@pytest.fixture
def routes_by_stop(
    stops_by_route: dict[str, list[str]],
) -> dict[str, set[str]]:
    return create_routes_by_stop(stops_by_route)


@pytest.fixture
def idx_by_stop_by_route(
    stops_by_route: dict[str, list[str]],
) -> dict[str, dict[str, int]]:
    return create_idx_by_stop_by_route(stops_by_route)


@pytest.fixture
def times_by_stop_by_trip(
    stop_times_by_trip: dict[str, list[dict[str, str]]]
) -> dict[str, dict[str, tuple[int, int]]]:
    return create_times_by_stop_by_trip(stop_times_by_trip)


@pytest.fixture
def test_create_id_sets(
    trips_df: pd.DataFrame, routes_by_stop: dict[str, set[str]]
) -> tuple[set[str], set[str], set[str]]:
    return create_id_sets(trips_df, routes_by_stop)
