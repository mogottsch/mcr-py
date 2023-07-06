from typing_extensions import Self

from package import strtime


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

    def to_human_readable(self) -> set[tuple[str, str]]:
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


LabelWithTrip = tuple[Label, str]
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
