from typing import Optional, Tuple

from matplotlib import sys

from package import strtime
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
from package.logger import llog


def raptor(
    structs: dict,
    footpaths: dict,
    start_stop_id: str,
    end_stop_id: str,
    start_time_str: str,
    max_transfers: int,
    default_transfer_time: int,
):
    (
        stop_times_by_trip,
        trip_ids_by_route,
        stops_by_route,
        idx_by_stop_by_route,
        routes_by_stop,
        times_by_stop_by_trip,
        stop_id_set,
        route_id_set,
        trip_id_set,
    ) = unpack_structs(structs)

    start_time = strtime.str_time_to_seconds(start_time_str)

    # --- helper methods ---
    def get_routes_serving_stop(stop_id):
        return routes_by_stop[stop_id]

    def get_idx_of_stop_in_route(stop_id, route_id):
        return idx_by_stop_by_route[route_id][stop_id]

    def get_arrival_time(trip_id: str, stop_id: str) -> int:
        assert trip_id is not None
        assert stop_id is not None

        arrival_time = times_by_stop_by_trip[trip_id][stop_id][0]
        assert type(arrival_time) == int
        return arrival_time

    def get_departure_time(trip_id: Optional[str], stop_id: str) -> int:
        assert stop_id is not None

        if trip_id is None:
            return sys.maxsize

        departure_time = times_by_stop_by_trip[trip_id][stop_id][1]
        assert type(departure_time) == int
        return departure_time

    def earliest_trip(route_id: str, stop_id: str, arrival_time: int, change_time: int):
        trip_ids = trip_ids_by_route[route_id]
        for trip_id in trip_ids:
            if get_departure_time(trip_id, stop_id) >= arrival_time + change_time:
                return trip_id

    # --- end helper methods ---

    tau_i, tau_best, marked_stops = init(stop_id_set, start_stop_id, start_time)

    k = 0
    for k in range(1, max_transfers + 1):
        llog.debug(f"iteration {k}")
        Q = {}
        tau_i[k] = tau_i[k - 1].copy()

        for stop_id in marked_stops:
            for route_id in get_routes_serving_stop(stop_id):
                if route_id not in Q:
                    Q[route_id] = stop_id
                    continue

                # if our stop is closer to the start than the existing one, we replace it
                existing_stop_id = Q[route_id]
                idx = get_idx_of_stop_in_route(stop_id, route_id)
                existing_idx = get_idx_of_stop_in_route(existing_stop_id, route_id)
                if idx < existing_idx:
                    Q[route_id] = stop_id

        llog.debug(f"Q: {Q}")
        marked_stops = set()

        for route_id, stop_id in Q.items():
            trip_id = None
            for stop_id, _ in stops_by_route[route_id]:
                if trip_id is not None and get_arrival_time(trip_id, stop_id) < min(
                    tau_best[stop_id], tau_best[end_stop_id]
                ):
                    arrival_time = get_arrival_time(trip_id, stop_id)
                    assert type(arrival_time) == int
                    tau_i[k][stop_id] = arrival_time
                    tau_best[stop_id] = arrival_time
                    marked_stops.add(stop_id)

                ready_to_depart = tau_i[k - 1][stop_id] + default_transfer_time
                if ready_to_depart < get_departure_time(trip_id, stop_id):
                    trip_id = earliest_trip(
                        route_id, stop_id, ready_to_depart, default_transfer_time
                    )

        additional_marked_stops = set()
        for stop_id in marked_stops:
            for nearby_stop_id, walking_time in footpaths[stop_id].items():
                nearby_stop_arrival_time = tau_i[k][stop_id] + walking_time

                # we have a couple of modifications here:
                # 1. we only mark the stop if tau is actually updated
                # 2. we use tau_best instead of tau_k (should not make a difference)
                # 3. we also consider tau_best[end_stop_id] just like in the stop before
                # 4. we also update tau_best
                if nearby_stop_arrival_time < min(
                    tau_best[nearby_stop_id], tau_best[end_stop_id]
                ):
                    assert type(nearby_stop_arrival_time) == int
                    tau_i[k][nearby_stop_id] = nearby_stop_arrival_time
                    tau_best[nearby_stop_id] = nearby_stop_arrival_time
                    additional_marked_stops.add(nearby_stop_id)
        marked_stops.update(additional_marked_stops)

        llog.debug(f"marked_stops: {marked_stops}")
        llog.debug(f"tau_i: {tau_i[k]}")

        if len(marked_stops) == 0:
            break

    llog.debug(f"RAPTOR finished after {k} iterations")
    return seconds_dict_to_times_dict(tau_best)


def unpack_structs(structs: dict):
    return (
        structs[STOP_TIMES_BY_TRIP_KEY],
        structs[TRIP_IDS_BY_ROUTE_KEY],
        structs[STOPS_BY_ROUTE_KEY],
        structs[IDX_BY_STOP_BY_ROUTE_KEY],
        structs[ROUTES_BY_STOP_KEY],
        structs[TIMES_BY_STOP_BY_TRIP_KEY],
        structs[STOP_ID_SET_KEY],
        structs[ROUTE_ID_SET_KEY],
        structs[TRIP_ID_SET_KEY],
    )


def init(
    stop_id_set: set, start_stop_id: str, start_time: int
) -> Tuple[dict[int, dict[str, int]], dict[str, int], set[str]]:
    tau_i: dict[int, dict[str, int]] = {
        0: {},
    }
    tau_best = {}
    marked_stops = set()

    for stop_id in stop_id_set:
        tau_i[0][stop_id] = sys.maxsize
        tau_best[stop_id] = sys.maxsize

    tau_i[0][start_stop_id] = start_time
    tau_best[start_stop_id] = start_time

    marked_stops.add(start_stop_id)

    return tau_i, tau_best, marked_stops


def seconds_dict_to_times_dict(seconds_dict: dict[str, int]) -> dict[str, str]:
    return {
        stop_id: strtime.seconds_to_str_time(seconds)
        for stop_id, seconds in seconds_dict.items()
    }
