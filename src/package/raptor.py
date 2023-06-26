from typing import Optional, Tuple

from matplotlib import sys
from rich.pretty import pprint

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
from package.tracer import (
    TraceStart,
    TraceTrip,
    TraceFootpath,
    TracerMap,
)


def raptor(
    structs: dict,
    footpaths: dict,
    start_stop_id: str,
    end_stop_id: str,
    start_time_str: str,
    max_transfers: int,
    default_transfer_time: int,
) -> Tuple[dict[str, str], TracerMap]:
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

    def earliest_trip(
        route_id: str, stop_id: str, arrival_time: int, change_time: int
    ) -> Optional[Tuple[str, int]]:
        trip_ids = trip_ids_by_route[route_id]  # sorted by departure time
        for trip_id in trip_ids:
            departure_time = get_departure_time(trip_id, stop_id)
            if departure_time >= arrival_time + change_time:
                return trip_id, departure_time

    # --- end helper methods ---

    tau_i, tau_best, marked_stops, tracers_map = init(
        stop_id_set, start_stop_id, start_time
    )

    k = 0
    for k in range(1, max_transfers + 1):
        llog.debug(f"iteration {k}")
        Q: dict[str, tuple[str, int]] = {}
        tau_i[k] = tau_i[k - 1].copy()

        for stop_id in marked_stops:
            for route_id in get_routes_serving_stop(stop_id):
                idx = get_idx_of_stop_in_route(stop_id, route_id)
                if route_id not in Q:
                    Q[route_id] = (stop_id, idx)
                    continue

                # if our stop is closer to the start than the existing one, we replace it
                _, existing_idx = Q[route_id]
                if idx < existing_idx:
                    Q[route_id] = (stop_id, idx)

        llog.debug(f"Q: {Q}")
        marked_stops = set()

        for route_id, (stop_id, idx) in Q.items():
            trip_id: Optional[str] = None
            hop_on_stop_id: Optional[str] = None
            hop_on_time: Optional[int] = None
            for stop_id in stops_by_route[route_id][idx:]:
                if trip_id is not None and get_arrival_time(trip_id, stop_id) < min(
                    tau_best[stop_id], tau_best[end_stop_id]
                ):
                    arrival_time = get_arrival_time(trip_id, stop_id)
                    assert type(arrival_time) == int
                    assert arrival_time >= start_time
                    tau_i[k][stop_id] = arrival_time
                    tau_best[stop_id] = arrival_time

                    if hop_on_stop_id is None or hop_on_time is None:
                        raise Exception(
                            "hop_on_stop_id or hop_on_time should not be None if trip_id is not None"
                        )
                    tracers_map.add(
                        TraceTrip(
                            start_stop_id=hop_on_stop_id,
                            end_stop_id=stop_id,
                            departure_time=hop_on_time,
                            arrival_time=arrival_time,
                            trip_id=trip_id,
                        ),
                    )

                    marked_stops.add(stop_id)

                ready_to_depart = tau_i[k - 1][stop_id] + default_transfer_time
                if ready_to_depart < get_departure_time(trip_id, stop_id):
                    result = earliest_trip(
                        route_id, stop_id, ready_to_depart, default_transfer_time
                    )
                    if result is None:
                        continue
                    trip_id, hop_on_time = result
                    hop_on_stop_id = stop_id

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
                    assert nearby_stop_arrival_time >= start_time
                    tau_i[k][nearby_stop_id] = nearby_stop_arrival_time
                    tau_best[nearby_stop_id] = nearby_stop_arrival_time
                    additional_marked_stops.add(nearby_stop_id)

                    tracers_map.add(
                        TraceFootpath(stop_id, nearby_stop_id, walking_time),
                    )

        marked_stops.update(additional_marked_stops)

        llog.debug(f"marked_stops: {marked_stops}")
        llog.debug(f"tau_i: {tau_i[k]}")

        if len(marked_stops) == 0:
            break

    llog.info(f"RAPTOR finished after {k} iterations")
    return seconds_dict_to_times_dict(tau_best), tracers_map


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
) -> Tuple[dict[int, dict[str, int]], dict[str, int], set[str], TracerMap,]:
    tau_i: dict[int, dict[str, int]] = {
        0: {},
    }
    tracer = TracerMap(stop_id_set)
    tau_best = {}
    marked_stops = set()

    for stop_id in stop_id_set:
        tau_i[0][stop_id] = sys.maxsize
        tau_best[stop_id] = sys.maxsize

    tau_i[0][start_stop_id] = start_time
    tau_best[start_stop_id] = start_time
    tracer.add(
        tracer=TraceStart(start_stop_id, start_time),
    )

    marked_stops.add(start_stop_id)

    return tau_i, tau_best, marked_stops, tracer


def seconds_dict_to_times_dict(seconds_dict: dict[str, int]) -> dict[str, str]:
    return {
        stop_id: strtime.seconds_to_str_time(seconds)
        for stop_id, seconds in seconds_dict.items()
    }
