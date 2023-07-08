from typing import Callable, Generic, TypeVar
from typing_extensions import Self

from package import strtime
from package.key import S, T
from package.raptor.data import ExpandedDataQuerier


class LabelInterface:
    def __init__(self, time: int, stop: str):
        self.arrival_time = time
        self.stop = stop

    def __repr__(self):
        return f"Label({self.arrival_time}, {self.stop})"

    def strictly_dominates(self, other: Self) -> bool:
        return True

    def update_along_trip(self, arrival_time: int, stop_id: str, trip_id: str):
        pass

    def update_along_footpath(self, walking_time: int, stop_id: str):
        pass

    def update_before_route_bag_merge(self, departure_time: int, stop_id: str):
        pass

    def to_human_readable(self):
        pass

    def copy(self: Self) -> Self:
        return LabelInterface(self.arrival_time, self.stop)


L = TypeVar("L", bound=LabelInterface)  # custom label


class Bag:
    def __init__(self):
        self._bag: set[LabelInterface] = set()

    def __iter__(self):
        return iter(self._bag)

    def __str__(self):
        return str(self._bag)

    def __repr__(self):
        return repr(self._bag)

    def add_if_necessary(self, label: LabelInterface) -> bool:
        if not self.content_dominates(label):
            self.remove_dominated_by(label)
            self.add(label)
            return True
        return False

    def add(self, label: LabelInterface):
        self._bag.add(label.copy())

    def content_dominates(self, label: LabelInterface):
        return any(other.strictly_dominates(label) for other in self._bag)

    def remove_dominated_by(self, label: LabelInterface):
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

    def copy(self):
        new_bag = RouteBag(
            self._dq,
            self._update_label_along_trip,
            self._update_label_along_footpath,
            self._update_label_along_waiting,
        )
        new_bag._bag = set((label.copy(), trip) for label, trip in self._bag)
        return new_bag
