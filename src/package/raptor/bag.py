from copy import deepcopy
from typing import Generic, Optional, TypeVar
from typing_extensions import Self

from package.key import S, T
from package.raptor.data import ExpandedDataQuerier
from package.tracer.tracer import TraceFootpath, TraceStart, TraceTrip


class BaseLabel:
    def __init__(self, time: int):
        self.arrival_time = time

    def __repr__(self):
        return f"Label({self.arrival_time})"

    def strictly_dominates(self, other: Self) -> bool:
        return True

    def update_along_trip(self, arrival_time: int, stop_id: str, trip_id: str):
        self.arrival_time = arrival_time

    def update_along_footpath(self, walking_time: int, stop_id: str):
        self.arrival_time = self.arrival_time + walking_time

    def update_before_route_bag_merge(self, departure_time: int, stop_id: str):
        self.arrival_time = departure_time

    def to_human_readable(self):
        pass

    def copy(self: Self) -> Self:
        return deepcopy(self)


L = TypeVar("L", bound=BaseLabel)  # custom label


class Bag:
    def __init__(self):
        self._bag: set[BaseLabel] = set()

    def __iter__(self):
        return iter(self._bag)

    def __str__(self):
        return str(self._bag)

    def __repr__(self):
        return repr(self._bag)

    def add_if_necessary(self, label: BaseLabel) -> bool:
        if not self.content_dominates(label):
            self.remove_dominated_by(label)
            self.add(label)
            return True
        return False

    def add(self, label: BaseLabel):
        self._bag.add(label.copy())

    def content_dominates(self, label: BaseLabel):
        return any(other.strictly_dominates(label) for other in self._bag)

    def remove_dominated_by(self, label: BaseLabel):
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

    def create_footpath_bag(
        self: Self,
        walk_time: int,
        stop_id: str,
    ):
        bag = self.copy()
        for label in bag._bag:
            label.update_along_footpath(walk_time, stop_id)
        return bag

    def add_arrival_time_to_all(self, time: int):
        for label in self._bag:
            label.arrival_time += time

    def to_human_readable(self):
        return list((label.to_human_readable()) for label in self._bag)

    def copy(self):
        new_bag = Bag()
        new_bag._bag = set(label.copy() for label in self._bag)
        return new_bag


ArrivalTimePerTrip = dict[str, int]


class RouteBag(Generic[L, S, T]):
    def __init__(
        self,
        dq: ExpandedDataQuerier[S, T],
    ):
        self._bag: set[tuple[L, str]] = set()
        self._dq = dq

    def __str__(self):
        return str(self._bag)

    def __repr__(self):
        return repr(self._bag)

    def add_if_necessary(self, label: L, trip: str):
        if not self.content_dominates(label):
            self.remove_dominated_by(label)
            self.add(label, trip)

    def add(self, label: L, trip: str):
        self._bag.add((label.copy(), trip))

    def content_dominates(self, label: L):
        return any(other.strictly_dominates(label) for other, _ in self._bag)

    def remove_dominated_by(self, label: L):
        self._bag = {
            (other_label, other_trip)
            for other_label, other_trip in self._bag
            if not label.strictly_dominates(other_label)
        }

    def update_along_trip(self, stop_id: str):
        for label, trip in self._bag:
            arrival_time = self._dq.get_arrival_time(trip, stop_id)
            label.update_along_trip(arrival_time, stop_id, trip)

    def get_trips(self) -> set[str]:
        return set(trip for _, trip in self._bag)

    def to_bag(self) -> Bag:
        bag = Bag()
        for label, _ in self._bag:
            bag.add(label)
        return bag


class TraceLabel(BaseLabel):
    def __init__(self, time: int, stop: Optional[str] = None):
        super().__init__(time)
        self.stops = []
        self.trips = []
        self.traces = []
        if stop is not None and time is not None:
            self.stops.append(stop)
            self.traces.append(TraceStart(stop, time))

        self.last_update = "start"

    def update_along_trip(self, arrival_time: int, stop_id: str, trip_id: str):
        old_arrival_time = self.arrival_time
        super().update_along_trip(arrival_time, stop_id, trip_id)
        if self.last_update == "trip":
            prev_trace = self.traces.pop()
            assert trip_id == prev_trace.trip_id
            self.traces.append(
                TraceTrip(
                    prev_trace.start_stop_id,
                    prev_trace.departure_time,
                    stop_id,
                    arrival_time,
                    trip_id,
                )
            )
        else:
            self.traces.append(
                TraceTrip(
                    self.stops[-1], old_arrival_time, stop_id, arrival_time, trip_id
                )
            )

        self.last_update = "trip"
        self.stops.append(stop_id)
        self.trips.append(trip_id)

    def update_along_footpath(self, walking_time: int, stop_id: str):
        super().update_along_footpath(walking_time, stop_id)
        self.last_update = "footpath"
        self.traces.append(TraceFootpath(self.stops[-1], stop_id, walking_time))
        self.stops.append(stop_id)

    def update_before_route_bag_merge(self, departure_time: int, stop_id: str):
        super().update_before_route_bag_merge(departure_time, stop_id)
