import sys
from typing import Generic, Optional
from typing_extensions import Self

from package.key import S, T
from package.structs import build


class DataQuerier:
    def __init__(self: Self, structs_dict: dict, footpaths: dict | None):
        self.footpaths = footpaths

        (
            # stop_times_by_trip,
            self.trip_ids_by_route,
            self.stops_by_route,
            self.idx_by_stop_by_route,
            self.routes_by_stop,
            self.times_by_stop_by_trip,
            self.stop_id_set,
            # route_id_set,
            # trip_id_set,
        ) = build.unpack_structs(structs_dict)

    def get_stop_ids(self) -> set[str]:
        return self.stop_id_set

    def get_routes_by_stop(self, stop_id: str) -> set[str]:
        return self.routes_by_stop[stop_id]

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

    def get_departure_time(self, trip_id: Optional[str], stop_id: str) -> int:
        assert stop_id is not None

        if trip_id is None:
            return sys.maxsize

        departure_time = self.times_by_stop_by_trip[trip_id][stop_id][1]
        assert type(departure_time) == int
        return departure_time

    def earliest_trip(
        self, route_id: str, stop_id: str, arrival_time: int
    ) -> Optional[tuple[str, int]]:
        trip_ids = self.trip_ids_by_route[route_id]  # sorted by departure time
        for trip_id in trip_ids:
            departure_time = self.get_departure_time(trip_id, stop_id)
            if departure_time >= arrival_time:
                return trip_id, departure_time

    def iterate_footpaths_from_stop(self, stop_id: str):
        if self.footpaths == None:
            raise Exception(
                "footpaths has to be defined when calling iterate_footpaths_from_stop"
            )
        return self.footpaths[stop_id].items()

    def iterate_stops_in_route_from_idx(
        self: Self, route_id: str, idx: int
    ) -> list[str]:
        return self.stops_by_route[route_id][idx:]


class ExpandedDataQuerier(Generic[S, T], DataQuerier):
    def __init__(
        self: Self,
        structs_dict: dict,
        footpaths: dict | None,
        additional_stop_information: dict[str, S],
        additional_trip_information: dict[str, T],
    ):
        self.additional_stop_information = additional_stop_information
        self.additional_trip_information = additional_trip_information
        super().__init__(structs_dict, footpaths)

    def get_stop(self, stop_id: str) -> S:
        return self.additional_stop_information[stop_id]

    def get_trip(self, trip_id: str) -> T:
        return self.additional_trip_information[trip_id]
