# add . to module name
# import sys
# sys.path.append("./src/")

from typing import Optional
from package import storage, strtime
from package.structs import build
from package.raptor.mcraptor import McRaptor
from package.raptor import bag
from typing_extensions import Self

footpaths_dict = storage.read_any_dict("../data/footpaths.pkl")
footpaths_dict = footpaths_dict["footpaths"]

structs_dict = storage.read_any_dict("../data/structs.pkl")
build.validate_structs_dict(structs_dict)


class CustomLabel(bag.LabelInterface):
    def __init__(self, time: int, stop: Optional[str] = None):
        self.arrival_time = time
        self.travel_time = 0
        self.walking_time = 0
        self.waiting_time = 0
        self.stops = []
        self.trips = []
        if stop is not None:
            self.stops.append(stop)

        self.last_update = "start"

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
        interval = arrival_time - self.arrival_time
        if interval < 0:
            print(self)
        # assert interval >= 0
        self.arrival_time = arrival_time
        self.travel_time += interval
        self.last_update = "trip"
        self.stops.append(stop_id)
        self.trips.append(trip_id)

    def update_along_footpath(self, walking_time: int, stop_id: str):
        self.arrival_time = self.arrival_time + walking_time
        self.walking_time += walking_time
        self.last_update = "footpath"
        self.stops.append(stop_id)

    def update_before_route_bag_merge(self, departure_time: int, stop_id: str):
        interval = departure_time - self.arrival_time
        if interval < 0:
            print(self)
        # assert interval >= 0
        self.arrival_time = departure_time
        self.waiting_time += interval
        self.last_update = "waiting"
        self.stops.append(stop_id)

    def to_human_readable(self):
        return {
            "arrival_time": strtime.seconds_to_str_time(self.arrival_time),
            "travel_time": strtime.seconds_to_str_time(self.travel_time),
            "walking_time": strtime.seconds_to_str_time(self.walking_time),
            "waiting_time": strtime.seconds_to_str_time(self.waiting_time),
        }

    def copy(self: Self) -> Self:
        label = CustomLabel(self.arrival_time)

        self.stops = self.stops.copy()
        self.trips = self.trips.copy()
        self.last_update = self.last_update

        label.travel_time = self.travel_time
        label.walking_time = self.walking_time
        label.waiting_time = self.waiting_time

        return label


label_class = CustomLabel

mcraptor = McRaptor(structs_dict, footpaths_dict, 2, 60, {}, {}, label_class)
mcraptor.run("818", "", "15:00:00")
print("success")
