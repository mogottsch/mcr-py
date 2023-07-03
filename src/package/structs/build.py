from typing import Any
import pandas as pd
from package.logger import Timed

from package.strtime import str_time_to_seconds
from package.key import (
    STOP_TIMES_BY_TRIP_KEY,
    TRIP_IDS_BY_ROUTE_KEY,
    STOPS_BY_ROUTE_KEY,
    ROUTES_BY_STOP_KEY,
    IDX_BY_STOP_BY_ROUTE_KEY,
    TIMES_BY_STOP_BY_TRIP_KEY,
    STOP_ID_SET_KEY,
    ROUTE_ID_SET_KEY,
    TRIP_ID_SET_KEY,
)

STRUCTS_KEYS = [
    STOP_TIMES_BY_TRIP_KEY,
    TRIP_IDS_BY_ROUTE_KEY,
    STOPS_BY_ROUTE_KEY,
    ROUTES_BY_STOP_KEY,
    IDX_BY_STOP_BY_ROUTE_KEY,
    TIMES_BY_STOP_BY_TRIP_KEY,
    STOP_ID_SET_KEY,
    ROUTE_ID_SET_KEY,
    TRIP_ID_SET_KEY,
]


def build_structures(
    trips_df: pd.DataFrame, stop_times_df: pd.DataFrame
) -> dict[str, Any]:
    with Timed.info("Creating `stop_times_by_trip`"):
        stop_times_by_trip = create_stop_times_by_trip(stop_times_df)
    with Timed.info("Creating `trip_ids_by_route`"):
        trip_ids_by_route = create_trip_ids_by_route_sorted_by_departure(trips_df)
    with Timed.info("Creating `stops_by_route`"):
        stops_by_route = create_stops_by_route_ordered(
            trip_ids_by_route, stop_times_by_trip
        )
    with Timed.info("Creating `routes_by_stop`"):
        routes_by_stop = create_routes_by_stop(stops_by_route)
    with Timed.info("Creating `idx_by_stop_by_route`"):
        idx_by_stop_by_route = create_idx_by_stop_by_route(stops_by_route)
    with Timed.info("Creating `times_by_stop_by_trip`"):
        times_by_stop_by_trip = create_times_by_stop_by_trip(stop_times_by_trip)

    with Timed.info("Creating id sets"):
        stop_id_set, route_id_set, trip_id_set = create_id_sets(
            trips_df, routes_by_stop
        )

    data = {
        STOP_TIMES_BY_TRIP_KEY: stop_times_by_trip,
        TRIP_IDS_BY_ROUTE_KEY: trip_ids_by_route,
        STOPS_BY_ROUTE_KEY: stops_by_route,
        ROUTES_BY_STOP_KEY: routes_by_stop,
        IDX_BY_STOP_BY_ROUTE_KEY: idx_by_stop_by_route,
        TIMES_BY_STOP_BY_TRIP_KEY: times_by_stop_by_trip,
        STOP_ID_SET_KEY: stop_id_set,
        ROUTE_ID_SET_KEY: route_id_set,
        TRIP_ID_SET_KEY: trip_id_set,
    }
    return data


StopTimesByTrip = dict[str, list[dict[str, str]]]


def create_stop_times_by_trip(
    stop_times_df: pd.DataFrame,
) -> StopTimesByTrip:
    with Timed.debug("grouping by trip and sorting by stop sequence"):
        stop_times_by_trip_df = stop_times_df.groupby("trip_id").apply(
            lambda x: x.sort_values("stop_sequence")
        )[["arrival_time", "departure_time", "stop_id", "stop_sequence"]]

    stop_times_by_trip: StopTimesByTrip = {}

    with Timed.debug("creating stop_times_by_trip dictionary from dataframe"):
        for (trip_id, _), data in stop_times_by_trip_df.to_dict("index").items():
            stop_times = stop_times_by_trip.get(trip_id, [])
            stop_times.append(data)
            stop_times_by_trip[trip_id] = stop_times

    return stop_times_by_trip


TripIdsByRouteSortedByDeparture = dict[str, list[str]]


def create_trip_ids_by_route_sorted_by_departure(
    trips_df: pd.DataFrame,
) -> TripIdsByRouteSortedByDeparture:
    return (
        trips_df.sort_values("trip_departure_time")
        .groupby("route_id")["trip_id"]
        .apply(lambda x: x.tolist())
        .to_dict()
    )


StopsByRouteOrdered = dict[str, list[str]]


def create_stops_by_route_ordered(
    trip_ids_by_route: TripIdsByRouteSortedByDeparture,
    stop_times_by_trip: StopTimesByTrip,
) -> StopsByRouteOrdered:
    stops_by_route: StopsByRouteOrdered = {}
    for route_id, trip_ids in trip_ids_by_route.items():
        stops_ordered: list[str] = []
        # we only need the ordered stops, but use the set to check for duplicates
        stops = set()
        for trip_id in trip_ids:
            trip_stop_times = stop_times_by_trip[trip_id]

            for stop_time in trip_stop_times:
                stop = stop_time["stop_id"]
                if stop not in stops:
                    stops_ordered.append(stop)
                    stops.add(stop)

        stops_by_route[route_id] = stops_ordered

    return stops_by_route


RoutesByStop = dict[str, set[str]]


def create_routes_by_stop(stops_by_route: StopsByRouteOrdered) -> RoutesByStop:
    routes_by_stop: RoutesByStop = {}
    for route_id, stops in stops_by_route.items():
        for stop_id in stops:
            routes = routes_by_stop.get(stop_id, set())
            routes.add(route_id)
            routes_by_stop[stop_id] = routes

    assert type(list(routes_by_stop.keys())[0]) == str

    return routes_by_stop


StopIdSet = set[str]
RouteIdSet = set[str]
TripIdSet = set[str]
IdSets = tuple[StopIdSet, RouteIdSet, TripIdSet]


def create_id_sets(trips_df: pd.DataFrame, routes_by_stop: RoutesByStop) -> IdSets:
    stop_id_set = set(routes_by_stop.keys())  # some stops are not part of any trip
    route_id_set = set(trips_df["route_id"].unique())
    trip_id_set = set(trips_df["trip_id"].unique())

    return stop_id_set, route_id_set, trip_id_set


IdxByStopByRoute = dict[str, dict[str, int]]


def create_idx_by_stop_by_route(
    stops_by_route: StopsByRouteOrdered,
) -> IdxByStopByRoute:
    idx_by_stop_by_route = {
        k: {stop: idx for idx, stop in enumerate(v)} for k, v in stops_by_route.items()
    }
    return idx_by_stop_by_route


TimesByStopByTrip = dict[str, dict[str, tuple[int, int]]]


def create_times_by_stop_by_trip(
    stop_times_by_trip: StopTimesByTrip,
) -> TimesByStopByTrip:
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


def validate_structs_dict(structs: dict):
    for key in STRUCTS_KEYS:
        if not key in structs:
            raise Exception(f"Structs dict missing key {key}")


def unpack_structs(
    structs: dict,
) -> tuple[
    TripIdsByRouteSortedByDeparture,
    StopsByRouteOrdered,
    IdxByStopByRoute,
    RoutesByStop,
    TimesByStopByTrip,
    StopIdSet,
]:
    validate_structs_dict(structs)
    return (
        # structs[STOP_TIMES_BY_TRIP_KEY],
        structs[TRIP_IDS_BY_ROUTE_KEY],
        structs[STOPS_BY_ROUTE_KEY],
        structs[IDX_BY_STOP_BY_ROUTE_KEY],
        structs[ROUTES_BY_STOP_KEY],
        structs[TIMES_BY_STOP_BY_TRIP_KEY],
        structs[STOP_ID_SET_KEY],
        # structs[ROUTE_ID_SET_KEY],
        # structs[TRIP_ID_SET_KEY],
    )
