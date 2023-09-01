from typing import Optional
from typing_extensions import Self

from package import strtime
from package.raptor import bag


class ArrivalTimeLabel(bag.TraceLabel):
    """
    Label class for McRAPTOR algorithm, that only considers arrival time and therefore
    behaves like the original RAPTOR algorithm.
    """

    def __init__(self, time: int, stop: Optional[str] = None):
        super().__init__(time, stop)

    def strictly_dominates(self, other: Self) -> bool:
        return self.arrival_time <= other.arrival_time

    def update_along_trip(self, arrival_time: int, stop_id: str, trip_id: str):
        super().update_along_trip(arrival_time, stop_id, trip_id)

    def update_along_footpath(self, walking_time: int, stop_id: str):
        super().update_along_footpath(walking_time, stop_id)

    def update_before_route_bag_merge(self, departure_time: int, stop_id: str):
        super().update_before_route_bag_merge(departure_time, stop_id)

    def to_human_readable(self):
        return {
            "arrival_time": strtime.seconds_to_str_time(self.arrival_time),
            "stops": self.stops,
            "trips": self.trips,
            "traces": self.traces,
        }


class ActivityDurationLabel(bag.TraceLabel):
    def __init__(self, time: int, stop: Optional[str] = None):
        super().__init__(time, stop)
        self.travel_time = 0
        self.walking_time = 0
        self.waiting_time = 0

    def __repr__(self):
        return f"Label({self.arrival_time}, t={self.travel_time}, w={self.walking_time}, wait={self.waiting_time})"

    def strictly_dominates(self, other: Self) -> bool:
        return (
            self.arrival_time <= other.arrival_time
            and self.travel_time <= other.travel_time
            and self.walking_time <= other.walking_time
            and self.waiting_time <= other.waiting_time
        )

    def update_along_trip(self, arrival_time: int, stop_id: str, trip_id: str):
        old_arrival_time = self.arrival_time
        super().update_along_trip(arrival_time, stop_id, trip_id)
        interval = arrival_time - old_arrival_time
        assert interval >= 0
        self.travel_time += interval

    def update_along_footpath(self, walking_time: int, stop_id: str):
        super().update_along_footpath(walking_time, stop_id)
        self.walking_time += walking_time

    def update_before_route_bag_merge(self, departure_time: int, stop_id: str):
        old_arrival_time = self.arrival_time
        super().update_before_route_bag_merge(departure_time, stop_id)
        interval = departure_time - old_arrival_time
        assert interval >= 0
        self.waiting_time += interval

    def to_human_readable(self):
        return {
            "arrival_time": strtime.seconds_to_str_time(self.arrival_time),
            "travel_time": strtime.seconds_to_str_time(self.travel_time),
            "walking_time": strtime.seconds_to_str_time(self.walking_time),
            "waiting_time": strtime.seconds_to_str_time(self.waiting_time),
            "stops": self.stops,
            "trips": self.trips,
            "traces": self.traces,
        }
