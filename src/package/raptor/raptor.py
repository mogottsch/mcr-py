from typing import Optional
from typing_extensions import Self

import sys

from package import strtime
from package.logger import llog
from package.raptor.data import DataQuerier
from package.tracer.tracer import (
    TraceStart,
    TraceTrip,
    TraceFootpath,
    TracerMap,
)


MarkedRouteStopTuples = dict[str, tuple[str, int]]
TausPerIteration = dict[int, dict[str, int]]
TausBest = dict[str, int]


class Raptor:
    def __init__(
        self: Self,
        structs_dict: dict,
        footpaths: dict,
        max_transfers: int,
        default_transfer_time: int,
    ):
        self.dq = DataQuerier(structs_dict, footpaths)

        self.max_transfers = max_transfers
        self.default_transfer_time = default_transfer_time

    def run(self, start_stop_id: str, end_stop_id: Optional[str], start_time_str: str):
        start_time = strtime.str_time_to_seconds(start_time_str)

        tau_i, tau_best, marked_stops, tracers_map = self.init_vars(
            start_stop_id, start_time
        )

        k = 0
        for k in range(1, self.max_transfers + 1):
            llog.debug(f"iteration {k}")
            tau_i[k] = tau_i[k - 1].copy()

            Q = self.collect_Q(marked_stops)

            marked_stops, tau_i, tau_best, tracers_map = self.process_routes(
                Q, k, tau_i, tau_best, end_stop_id, tracers_map
            )

            (
                additional_marked_stops,
                tau_i,
                tau_best,
                tracers_map,
            ) = self.process_footpaths(
                marked_stops, k, tau_i, tau_best, end_stop_id, start_time, tracers_map
            )

            marked_stops.update(additional_marked_stops)

            llog.debug(f"marked_stops: {marked_stops}")
            llog.debug(f"tau_i: {tau_i[k]}")

            if len(marked_stops) == 0:
                break

        llog.info(f"RAPTOR finished after {k} iterations")
        return seconds_dict_to_times_dict(tau_best), tracers_map

    def collect_Q(self, marked_stops: set[str]) -> dict[str, tuple[str, int]]:
        Q: dict[str, tuple[str, int]] = {}
        for stop_id in marked_stops:
            for route_id in self.dq.get_routes_serving_stop(stop_id):
                idx = self.dq.get_idx_of_stop_in_route(stop_id, route_id)
                if route_id not in Q:
                    Q[route_id] = (stop_id, idx)
                    continue

                # if our stop is closer to the start than the existing one, we replace it
                _, existing_idx = Q[route_id]
                if idx < existing_idx:
                    Q[route_id] = (stop_id, idx)
        return Q

    def process_routes(
        self: Self,
        Q: MarkedRouteStopTuples,
        k: int,
        tau_i: TausPerIteration,
        tau_best: TausBest,
        end_stop_id: Optional[str],
        tracers_map: TracerMap,
    ) -> tuple[set[str], TausPerIteration, TausBest, TracerMap]:
        marked_stops = set()

        for route_id, (stop_id, idx) in Q.items():
            trip_id: Optional[str] = None

            tracers_map.clear_last_hop()

            for stop_id in self.dq.iterate_stops_in_route_from_idx(route_id, idx):
                trip_id, tau_i, tau_best, tracers_map = self.process_route(
                    route_id,
                    trip_id,
                    stop_id,
                    marked_stops,
                    k,
                    tau_i,
                    tau_best,
                    end_stop_id,
                    tracers_map,
                )

        return marked_stops, tau_i, tau_best, tracers_map

    def process_route(
        self: Self,
        route_id: str,
        trip_id: Optional[str],
        stop_id: str,
        marked_stops: set[str],
        k: int,
        tau_i: TausPerIteration,
        tau_best: TausBest,
        end_stop_id: Optional[str],
        tracers_map: TracerMap,
    ) -> tuple[Optional[str], TausPerIteration, TausBest, TracerMap]:
        tau_best_end_stop_id = tau_best[end_stop_id] if end_stop_id else sys.maxsize

        if route_id == "100013_1_A":
            print(route_id)

        if trip_id is not None and self.dq.get_arrival_time(trip_id, stop_id) < min(
            tau_best[stop_id], tau_best_end_stop_id
        ):
            self.update_tau_through_trip(
                trip_id, stop_id, k, tau_i, tau_best, tracers_map
            )
            marked_stops.add(stop_id)

        ready_to_depart = (
            tau_i[k - 1][stop_id] + self.default_transfer_time
        )  # TODO: this is wrong: used twice!
        if ready_to_depart < self.dq.get_departure_time(trip_id, stop_id):
            new_trip_id = self.find_earlier_trip(
                route_id, stop_id, ready_to_depart, tracers_map
            )
            trip_id = new_trip_id if new_trip_id is not None else trip_id

        return trip_id, tau_i, tau_best, tracers_map

    def update_tau_through_trip(
        self: Self,
        trip_id: str,
        stop_id: str,
        k: int,
        tau_i: TausPerIteration,
        tau_best: TausBest,
        tracers_map: TracerMap,
    ) -> None:
        arrival_time = self.dq.get_arrival_time(trip_id, stop_id)
        tau_i[k][stop_id] = arrival_time
        tau_best[stop_id] = arrival_time

        hop_on_stop_id, hop_on_time = tracers_map.get_last_hop()
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

    def find_earlier_trip(
        self: Self,
        route_id: str,
        stop_id: str,
        ready_to_depart: int,
        tracers_map: TracerMap,
    ) -> Optional[str]:
        result = self.dq.earliest_trip(
            route_id,
            stop_id,
            ready_to_depart,
        )
        if result is None:
            # we could not find a new trip, so we return as is
            # return trip_id, tau_i, tau_best, tracers_map
            return None
        trip_id, hop_on_time = result
        hop_on_stop_id = stop_id
        tracers_map.update_last_hop(hop_on_stop_id, hop_on_time)

        return trip_id

    def process_footpaths(
        self: Self,
        marked_stops: set[str],
        k: int,
        tau_i: TausPerIteration,
        tau_best: TausBest,
        end_stop_id: Optional[str],
        start_time: int,
        tracers_map: TracerMap,
    ) -> tuple[set[str], TausPerIteration, TausBest, TracerMap]:
        additional_marked_stops = set()
        for stop_id in marked_stops:
            for nearby_stop_id, walking_time in self.dq.iterate_footpaths_from_stop(
                stop_id
            ):
                nearby_stop_arrival_time = tau_i[k][stop_id] + walking_time

                tau_best_end_stop_id = (
                    tau_best[end_stop_id] if end_stop_id else sys.maxsize
                )
                # we have a couple of modifications here:
                # 1. we only mark the stop if tau is actually updated
                # 2. we use tau_best instead of tau_k (should not make a difference)
                # 3. we also consider tau_best[end_stop_id] just like in the stop before
                # 4. we also update tau_best
                if nearby_stop_arrival_time < min(
                    tau_best[nearby_stop_id], tau_best_end_stop_id
                ):
                    assert type(nearby_stop_arrival_time) == int
                    assert nearby_stop_arrival_time >= start_time
                    tau_i[k][nearby_stop_id] = nearby_stop_arrival_time
                    tau_best[nearby_stop_id] = nearby_stop_arrival_time
                    additional_marked_stops.add(nearby_stop_id)

                    tracers_map.add(
                        TraceFootpath(stop_id, nearby_stop_id, walking_time),
                    )
        return additional_marked_stops, tau_i, tau_best, tracers_map

    def init_vars(
        self, start_stop_id: str, start_time: int
    ) -> tuple[dict[int, dict[str, int]], dict[str, int], set[str], TracerMap]:
        tau_i: dict[int, dict[str, int]] = {
            0: {},
        }
        tracer = TracerMap(self.dq.get_stop_ids())
        tau_best = {}
        marked_stops = set()

        for stop_id in self.dq.get_stop_ids():
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
