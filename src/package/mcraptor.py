from typing import Optional, Tuple
from typing_extensions import Self
import sys

from package import strtime
from package.logger import llog
from package.structs import build
from package.tracer.tracer import (
    TraceStart,
    TraceFootpath,
    TracerMap,
)


class Label:
    def __init__(self, arrival_time: int, seconds_walked: int):
        self.arrival_time = arrival_time
        self.seconds_walked = seconds_walked

    def strictly_dominates(self, other: Self):
        return (
            self.arrival_time <= other.arrival_time
            and self.seconds_walked <= other.seconds_walked
            # and (
            #     self.arrival_time < other.arrival_time
            #     or self.seconds_walked < other.seconds_walked
            # )
        )

    def __repr__(self):
        return f"Label({self.arrival_time}, {self.seconds_walked})"

    def copy(self: Self) -> Self:
        return Label(self.arrival_time, self.seconds_walked)


class Bag:
    def __init__(self):
        self._bag: set[Label] = set()

    def __iter__(self):
        return iter(self._bag)

    def __str__(self):
        return str(self._bag)

    def __repr__(self):
        return repr(self._bag)

    def add_if_necessary(self, label: Label) -> bool:
        if not self.content_dominates(label):
            self.remove_dominated_by(label)
            self.add(label)
            return True
        return False

    def add(self, label: Label):
        self._bag.add(label.copy())

    def content_dominates(self, label: Label):
        return any(other.strictly_dominates(label) for other in self._bag)

    def remove_dominated_by(self, label: Label):
        self._bag = {
            other for other in self._bag if not label.strictly_dominates(other)
        }

    def merge(self: Self, other: Self) -> bool:
        is_any_added = False
        for label in other._bag:
            is_added = self.add_if_necessary(label)
            is_any_added = is_any_added or is_added
        return is_any_added

    def create_bag_with_timeoffset(self: Self, time: int) -> Self:
        bag = self.copy()
        bag.add_arrival_time_to_all(time)
        return bag

    def add_arrival_time_to_all(self, time: int):
        for label in self._bag:
            label.arrival_time += time

    def add_walking_time_to_all(self, time: int):
        for label in self._bag:
            label.seconds_walked += time

    def to_human_readable(self) -> set[Tuple[str, str]]:
        return set(
            (
                strtime.seconds_to_str_time(label.arrival_time),
                strtime.seconds_to_str_time(label.seconds_walked),
            )
            for label in self._bag
        )

    def copy(self):
        new_bag = Bag()
        new_bag._bag = set(label.copy() for label in self._bag)
        return new_bag


LabelWithTrip = Tuple[Label, str]
ArrivalTimePerTrip = dict[str, int]


class RouteBag:
    def __init__(self):
        self._bag: set[LabelWithTrip] = set()

    def __str__(self):
        return str(self._bag)

    def __repr__(self):
        return repr(self._bag)

    def add_if_necessary(self, label: Label, trip: str):
        if not self.content_dominates(label):
            self.remove_dominated_by(label)
            self.add(label, trip)

    def add(self, label: Label, trip: str):
        self._bag.add((label, trip))

    def content_dominates(self, label: Label):
        return any(other.strictly_dominates(label) for other, _ in self._bag)

    def remove_dominated_by(self, label: Label):
        self._bag = {
            (other_label, other_trip)
            for other_label, other_trip in self._bag
            if not label.strictly_dominates(other_label)
        }

    def update_arrival_times(self, arrival_times: ArrivalTimePerTrip):
        for label, trip in self._bag:
            label.arrival_time = arrival_times[trip]

    def get_trips(self) -> set[str]:
        return set(trip for _, trip in self._bag)

    def to_bag(self) -> Bag:
        bag = Bag()
        for label, _ in self._bag:
            bag.add(label)
        return bag

    def copy(self):
        new_bag = RouteBag()
        new_bag._bag = set((label.copy(), trip) for label, trip in self._bag)
        return new_bag


class McRaptor:
    def __init__(
        self,
        structs_dict: dict,
        footpaths: dict,
        max_transfers: int,
        default_transfer_time: int,
    ):
        (
            # stop_times_by_trip,
            trip_ids_by_route,
            stops_by_route,
            idx_by_stop_by_route,
            routes_by_stop,
            times_by_stop_by_trip,
            stop_id_set,
            # route_id_set,
            # trip_id_set,
        ) = build.unpack_structs(structs_dict)

        self.trip_ids_by_route = trip_ids_by_route
        self.stops_by_route = stops_by_route
        self.idx_by_stop_by_route = idx_by_stop_by_route
        self.routes_by_stop = routes_by_stop
        self.times_by_stop_by_trip = times_by_stop_by_trip
        self.stop_id_set = stop_id_set

        self.footpaths = footpaths
        self.max_transfers = max_transfers
        self.default_transfer_time = default_transfer_time

    def init_vars(
        self, start_stop_id: str, start_time: int
    ) -> Tuple[dict[int, dict[str, Bag]], dict[str, Bag], set[str], TracerMap]:
        tau_i: dict[int, dict[str, Bag]] = {
            0: {},
        }
        tracer = TracerMap(self.stop_id_set)
        tau_best: dict[str, Bag] = {}
        marked_stops = set()

        for stop_id in self.stop_id_set:
            tau_i[0][stop_id] = Bag()
            tau_best[stop_id] = Bag()

        start_bag = Bag()
        start_bag.add_if_necessary(Label(start_time, 0))
        tau_i[0][start_stop_id] = start_bag
        tau_best[start_stop_id] = start_bag.copy()
        tracer.add(
            tracer=TraceStart(start_stop_id, start_time),
        )

        marked_stops.add(start_stop_id)

        return tau_i, tau_best, marked_stops, tracer

    def get_routes_serving_stop(self, stop_id: str) -> set[str]:
        return self.routes_by_stop[stop_id]

    def get_idx_of_stop_in_route(self, stop_id: str, route_id: str) -> int:
        return self.idx_by_stop_by_route[route_id][stop_id]

    def get_arrival_time(self, trip_id: str, stop_id: str) -> int:
        assert trip_id is not None
        assert stop_id is not None

        arrival_time = self.times_by_stop_by_trip[trip_id][stop_id][0]
        assert type(arrival_time) == int
        return arrival_time

    def get_arrival_times(self, trip_ids: set[str], stop_id: str) -> dict[str, int]:
        return {
            trip_id: self.get_arrival_time(trip_id, stop_id) for trip_id in trip_ids
        }

    def get_departure_time(self, trip_id: Optional[str], stop_id: str) -> int:
        assert stop_id is not None

        if trip_id is None:
            return sys.maxsize

        departure_time = self.times_by_stop_by_trip[trip_id][stop_id][1]
        assert type(departure_time) == int
        return departure_time

    def earliest_trip(
        self, route_id: str, stop_id: str, arrival_time: int, change_time: int
    ) -> Optional[Tuple[str, int]]:
        trip_ids = self.trip_ids_by_route[route_id]  # sorted by departure time
        for trip_id in trip_ids:
            departure_time = self.get_departure_time(trip_id, stop_id)
            if departure_time >= arrival_time + change_time:
                return trip_id, departure_time

    def merge_bag_into_route_bag(
        self,
        route_bag: RouteBag,
        bag: Bag,
        route_id: str,
        stop_id: str,
    ):
        for label in bag:
            res = self.earliest_trip(
                route_id,
                stop_id,
                label.arrival_time,
                self.default_transfer_time,
            )
            if res is None:
                continue
            trip, _ = res
            route_bag.add_if_necessary(label, trip)

    def collect_Q(
        self: Self,
        marked_stops: set[str],
    ) -> dict[str, tuple[str, int]]:
        Q: dict[str, tuple[str, int]] = {}
        for stop_id in marked_stops:
            for route_id in self.get_routes_serving_stop(stop_id):
                idx = self.get_idx_of_stop_in_route(stop_id, route_id)
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
            route_bag = RouteBag()

            for stop_id in self.stops_by_route[route_id][idx:]:
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
        active_trips = route_bag.get_trips()
        arrival_times = self.get_arrival_times(
            active_trips,
            stop_id,
        )
        route_bag.update_arrival_times(arrival_times)

        # second step - merge route_bag into stop_bag
        stop_bag = b_i[k][stop_id]
        is_any_added = stop_bag.merge(route_bag.to_bag())
        if is_any_added:
            marked_stops.add(stop_id)

        # third step - merge stop_bag into route_bag
        transfer_time_stop_bag = stop_bag.create_bag_with_timeoffset(
            self.default_transfer_time
            if k > 0
            else 0  # there is no transfer time for the first stop
        )
        self.merge_bag_into_route_bag(
            route_bag,
            transfer_time_stop_bag,
            route_id,
            stop_id,
        )
        return b_i, marked_stops, route_bag

    def process_footpaths(
        self: Self,
        marked_stops: set[str],
        k: int,
        b_i: dict[int, dict[str, Bag]],
    ) -> tuple[dict[int, dict[str, Bag]], set[str]]:
        additional_marked_stops = set()
        for stop_id in marked_stops:
            for nearby_stop_id, walking_time in self.footpaths[stop_id].items():
                start_bag = b_i[k][stop_id]
                walk_bag = start_bag.create_bag_with_timeoffset(walking_time)
                walk_bag.add_walking_time_to_all(walking_time)

                end_bag = b_i[k][nearby_stop_id]

                is_any_added = end_bag.merge(walk_bag)

                if is_any_added:
                    additional_marked_stops.add(nearby_stop_id)
        return b_i, additional_marked_stops

    # TODO: prune by end_stop_id
    def run(
        self, start_stop_id: str, end_stop_id: Optional[str], start_time_str: str
    ) -> dict[str, set[tuple[str, str]]]:
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

            llog.debug(f"marked_stops: {marked_stops}")
            llog.debug(f"tau_i: {b_i[k]}")

            if len(marked_stops) == 0:
                break

        llog.info(f"RAPTOR finished after {k} iterations")
        return bags_to_human_readable(b_best)


def bags_to_human_readable(bags: dict[str, Bag]) -> dict[str, set[tuple[str, str]]]:
    return {stop_id: bag.to_human_readable() for stop_id, bag in bags.items()}
