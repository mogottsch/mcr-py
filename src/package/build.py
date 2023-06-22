from typing import Any
import pandas as pd

from package.strtime import str_time_to_seconds
from package.key import (
    STOP_TIMES_BY_TRIP_KEY,
    TRIP_IDS_BY_ROUTE_KEY,
    STOPS_BY_ROUTE_KEY,
    ROUTES_BY_STOP_KEY,
    IDX_BY_STOP_BY_ROUTE_KEY,
    TIMES_BY_STOP_BY_ROUTE_KEY,
    STOP_ID_SET_KEY,
    ROUTE_ID_SET_KEY,
    TRIP_ID_SET_KEY,
)


def build_structures(
    trips_df: pd.DataFrame, stop_times_df: pd.DataFrame
) -> dict[str, Any]:
    stop_times_by_trip = create_stop_times_by_trip(stop_times_df)
    trip_ids_by_route = create_trip_ids_by_route(trips_df)
    stops_by_route = create_stops_by_route(trip_ids_by_route, stop_times_by_trip)
    routes_by_stop = create_routes_by_stop(stops_by_route)
    idx_by_stop_by_route = create_idx_by_stop_by_route(stops_by_route)
    times_by_stop_by_route = create_times_by_stop_by_trip(stop_times_by_trip)

    stop_id_set, route_id_set, trip_id_set = create_id_sets(trips_df, routes_by_stop)

    data = {
        STOP_TIMES_BY_TRIP_KEY: stop_times_by_trip,
        TRIP_IDS_BY_ROUTE_KEY: trip_ids_by_route,
        STOPS_BY_ROUTE_KEY: stops_by_route,
        ROUTES_BY_STOP_KEY: routes_by_stop,
        IDX_BY_STOP_BY_ROUTE_KEY: idx_by_stop_by_route,
        TIMES_BY_STOP_BY_ROUTE_KEY: times_by_stop_by_route,
        STOP_ID_SET_KEY: stop_id_set,
        ROUTE_ID_SET_KEY: route_id_set,
        TRIP_ID_SET_KEY: trip_id_set,
    }
    return data


def create_stop_times_by_trip(
    stop_times_df: pd.DataFrame,
) -> dict[str, list[dict[str, str]]]:
    stop_times_by_trip_df = stop_times_df.groupby("trip_id").apply(
        lambda x: x.sort_values("stop_sequence")
    )[["arrival_time", "departure_time", "stop_id", "stop_sequence"]]

    stop_times_by_trip: dict[str, list[dict[str, str]]] = {}

    for (trip_id, _), data in stop_times_by_trip_df.to_dict("index").items():
        stop_times = stop_times_by_trip.get(trip_id, [])
        stop_times.append(data)
        stop_times_by_trip[trip_id] = stop_times

    return stop_times_by_trip


def create_trip_ids_by_route(trips_df: pd.DataFrame) -> dict[str, list[str]]:
    return trips_df.groupby("route_id")["trip_id"].apply(lambda x: x.tolist()).to_dict()


def create_stops_by_route(
    trip_ids_by_route: dict[str, list[str]],
    stop_times_by_trip: dict[str, list[dict[str, str]]],
) -> dict[str, list[tuple[str, int]]]:
    stops_by_route: dict[str, list[tuple[str, int]]] = {}
    for route_id, trip_ids in trip_ids_by_route.items():
        stops_ordered = []
        stops = (
            set()
        )  # we only need the ordered stops, but use the set to check for duplicates
        for trip_id in trip_ids:
            trip_stop_times = stop_times_by_trip[trip_id]

            # stop_times_by_trip_dict = stop_times_by_trips_by_route.get(route_id, {})
            # stop_times_by_trip_dict[trip_id] = trip_stop_times
            # stop_times_by_trips_by_route[route_id] = stop_times_by_trip_dict

            for stop_time in trip_stop_times:
                stop_in_route = (stop_time["stop_id"], stop_time["stop_sequence"])
                if stop_in_route not in stops:
                    stops_ordered.append(stop_in_route)
                    stops.add(stop_in_route)

        stops_by_route[route_id] = stops_ordered

    return stops_by_route


def create_routes_by_stop(
    stops_by_route: dict[str, list[tuple[str, int]]]
) -> dict[str, list[str]]:
    routes_by_stop: dict[str, list[str]] = {}
    for route_id, stops in stops_by_route.items():
        for stop_id, _ in stops:
            routes = routes_by_stop.get(stop_id, [])
            routes.append(route_id)
            routes_by_stop[stop_id] = routes

    return routes_by_stop


def create_id_sets(
    trips_df: pd.DataFrame, routes_by_stop: dict[str, list[str]]
) -> tuple[set[str], set[str], set[str]]:
    stop_id_set = set(routes_by_stop.keys())  # some stops are not part of any trip
    route_id_set = set(trips_df["route_id"].unique())
    trip_id_set = set(trips_df["trip_id"].unique())

    return stop_id_set, route_id_set, trip_id_set


def create_idx_by_stop_by_route(
    stops_by_route: dict[str, list[tuple[str, int]]]
) -> dict[str, dict[str, int]]:
    idx_by_stop_by_route = {
        k: {stop: stop_seq for (stop, stop_seq) in (v)}
        for k, v in stops_by_route.items()
    }
    return idx_by_stop_by_route


def create_times_by_stop_by_trip(
    stop_times_by_trip: dict[str, list[dict[str, str]]]
) -> dict[str, dict[str, tuple[int, int]]]:
    return {
        trip_id: {
            stop["stop_id"]: (
                str_time_to_seconds(stop["arrival_time"]),
                str_time_to_seconds(stop["departure_time"]),
            )
            for stop in stops
        }
        for (trip_id, stops) in stop_times_by_trip.items()
    }


# def get_idx_of_stop_in_route(stop_id, route_id):
#     return stops_by_route_dict[route_id][stop_id]

# def get_arrival_time(trip_id: str, stop_id: str) -> int:
#     assert trip_id is not None
#     assert stop_id is not None
#
#     arrival_time = times_by_stop_by_trip[trip_id][stop_id][0]
#     assert type(arrival_time) == int
#     return arrival_time

# def get_departure_time(trip_id: str, stop_id: str) -> int:
#     assert stop_id is not None
#
#     if trip_id is None:
#         return sys.maxsize
#
#     departure_time = times_by_stop_by_trip[trip_id][stop_id][1]
#     assert type(departure_time) == int
#     return departure_time
