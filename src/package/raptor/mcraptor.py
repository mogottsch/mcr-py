from typing import Generic, Optional
from typing_extensions import Any, Self

from package import strtime
from package.key import S, T
from package.logger import llog
from package.raptor.bag import L, Bag, RouteBag
from package.raptor.data import ExpandedDataQuerier
from package.tracer.tracer import (
    TraceStart,
    TracerMap,
)


class McRaptor(Generic[L, S, T]):
    def __init__(
        self,
        structs_dict: dict,
        footpaths: dict,
        max_transfers: int,
        default_transfer_time: int,
        additional_stop_information: dict[str, S],
        additional_trip_information: dict[str, T],
        label_class: type[L],
    ):
        self.dq = ExpandedDataQuerier(
            structs_dict,
            footpaths,
            additional_stop_information,
            additional_trip_information,
        )

        self.max_transfers = max_transfers
        self.default_transfer_time = default_transfer_time

        self.label_class = label_class

    # TODO: prune by end_stop_id
    def run(
        self, start_stop_id: str, end_stop_id: Optional[str], start_time_str: str
    ) -> dict[str, Any]:
        start_time = strtime.str_time_to_seconds(start_time_str)

        b_i, b_best, marked_stops, tracers_map = self.init_vars(
            start_stop_id, start_time
        )

        k = 0
        for k in range(1, self.max_transfers + 1):
            llog.debug(f"iteration {k}")
            b_i[k] = b_i[k - 1].copy()

            Q = self.collect_Q(marked_stops)

            b_i, marked_stops = self.process_routes(Q, k, b_i)
            b_i, additional_marked_stops = self.process_footpaths(marked_stops, k, b_i)

            marked_stops.update(additional_marked_stops)

            # copy current bags into b_best
            b_best = {stop_id: bag.copy() for stop_id, bag in b_i[k].items()}

            llog.debug(f"marked_stops: {len(marked_stops)}")

            if len(marked_stops) == 0:
                break

        llog.info(f"RAPTOR finished after {k} iterations")
        return bags_to_human_readable(b_best)

    def init_vars(
        self, start_stop_id: str, start_time: int
    ) -> tuple[dict[int, dict[str, Bag]], dict[str, Bag], set[str], TracerMap]:
        tau_i: dict[int, dict[str, Bag]] = {
            0: {},
        }
        tracer = TracerMap(self.dq.stop_id_set)
        tau_best: dict[str, Bag] = {}
        marked_stops = set()

        for stop_id in self.dq.stop_id_set:
            tau_i[0][stop_id] = Bag()
            tau_best[stop_id] = Bag()

        start_bag = Bag()
        start_bag.add_if_necessary(self.label_class(start_time, start_stop_id))
        tau_i[0][start_stop_id] = start_bag
        tau_best[start_stop_id] = start_bag.copy()
        tracer.add(
            tracer=TraceStart(start_stop_id, start_time),
        )

        marked_stops.add(start_stop_id)

        return tau_i, tau_best, marked_stops, tracer

    def collect_Q(
        self: Self,
        marked_stops: set[str],
    ) -> dict[str, tuple[str, int]]:
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
        Q: dict[str, tuple[str, int]],
        k: int,
        b_i: dict[int, dict[str, Bag]],
    ) -> tuple[dict[int, dict[str, Bag]], set[str]]:
        marked_stops = set()
        for route_id, (stop_id, idx) in Q.items():
            route_bag = RouteBag[L, S, T](
                self.dq,
            )

            for stop_id in self.dq.iterate_stops_in_route_from_idx(route_id, idx):
                b_i, marked_stops, route_bag = self.process_route(
                    route_id,
                    stop_id,
                    k,
                    b_i,
                    route_bag,
                    marked_stops,
                )
        return b_i, marked_stops

    def process_route(
        self,
        route_id: str,
        stop_id: str,
        k: int,
        b_i: dict[int, dict[str, Bag]],
        route_bag: RouteBag,
        marked_stops: set[str],
    ) -> tuple[dict[int, dict[str, Bag]], set[str], RouteBag]:
        # first step - update arrival times in route bag
        route_bag.update_along_trip(stop_id)

        # second step - merge route_bag into stop_bag
        stop_bag = b_i[k][stop_id]
        is_any_added = stop_bag.merge(route_bag.to_bag())
        if is_any_added:
            marked_stops.add(stop_id)

        # third step - merge stop_bag into route_bag
        self.merge_bag_into_route_bag(
            route_bag,
            stop_bag,
            route_id,
            stop_id,
        )
        return b_i, marked_stops, route_bag

    # TODO: should this be moved into RouteBag?
    def merge_bag_into_route_bag(
        self,
        route_bag: RouteBag,
        bag: Bag,
        route_id: str,
        stop_id: str,
    ):
        for label in bag:
            res = self.dq.earliest_trip(
                route_id,
                stop_id,
                label.arrival_time + self.default_transfer_time,
            )
            if res is None:
                continue
            trip, departure_time = res
            label = label.copy()
            label.update_before_route_bag_merge(departure_time, stop_id)
            route_bag.add_if_necessary(label, trip)

    def process_footpaths(
        self: Self,
        marked_stops: set[str],
        k: int,
        b_i: dict[int, dict[str, Bag]],
    ) -> tuple[dict[int, dict[str, Bag]], set[str]]:
        additional_marked_stops = set()
        for stop_id in marked_stops:
            for nearby_stop_id, walking_time in self.dq.footpaths[stop_id].items():
                start_bag = b_i[k][stop_id]
                footpath_bag = start_bag.create_footpath_bag(
                    walking_time,
                    nearby_stop_id,
                )

                end_bag = b_i[k][nearby_stop_id]

                is_any_added = end_bag.merge(footpath_bag)

                if is_any_added:
                    additional_marked_stops.add(nearby_stop_id)
        return b_i, additional_marked_stops


def bags_to_human_readable(bags: dict[str, Bag]) -> dict[str, Any]:
    return {stop_id: bag.to_human_readable() for stop_id, bag in bags.items()}
