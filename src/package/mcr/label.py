from __future__ import annotations

from package.raptor.bag import BaseLabel as McRAPTORBaseLabel

COST_SHORT_DISTANCE_TICKET_INCR = 220
COST_LONG_DISTANCE_TICKET_INCR = 320

LENGTH_SHORT_DISTANCE_TICKET = 4


class IntermediateLabel:
    def __init__(
        self,
        values: list[int],
        hidden_values: list[int],
        path: list[int | str],
        osm_node_id: int,
    ):
        self.values = values
        self.hidden_values = hidden_values
        self.path = path
        self.node_id = osm_node_id

    def __str__(self):
        return f"IntermediateLabel(values={self.values}, hidden_values={self.hidden_values}, path={self.path}, node_id={self.node_id})"

    def __repr__(self):
        return str(self)

    def strictly_dominates(self, other: IntermediateLabel) -> bool:
        assert len(self.values) == len(other.values)
        return all([self.values[i] <= other.values[i] for i in range(len(self.values))])

    def copy_with_node_id(self, node_id: int) -> IntermediateLabel:
        return IntermediateLabel(
            values=self.values.copy(),
            hidden_values=self.hidden_values.copy(),
            path=self.path.copy(),
            osm_node_id=node_id,
        )

    def to_mlc_label(self, new_node_id: int) -> IntermediateLabel:
        return IntermediateLabel(
            values=self.values.copy(),
            hidden_values=self.hidden_values.copy(),
            path=self.path.copy(),
            osm_node_id=new_node_id,
        )

    def to_mc_raptor_label(self, stop_id: str) -> McRAPTORLabel:
        n_stops = self.hidden_values[1] if len(self.hidden_values) > 1 else 0
        if len(self.path) > 0:
            return McRAPTORLabelWithPath(
                time=self.values[0],
                cost=self.values[1],
                stop=stop_id,
                path=self.path,
                n_stops=n_stops,
            )
        return McRAPTORLabel(
            time=self.values[0],
            cost=self.values[1],
            stop=stop_id,
            n_stops=n_stops,
        )


def merge_intermediate_bags(
    bag: list[IntermediateLabel],
    other_bag: list[IntermediateLabel],
) -> list[IntermediateLabel]:
    merged_bag = []
    for label in bag:
        if not any(
            [other_label.strictly_dominates(label) for other_label in other_bag]
        ):
            merged_bag.append(label)
    for label in other_bag:
        if not any([other_label.strictly_dominates(label) for other_label in bag]):
            merged_bag.append(label)
    return merged_bag


class McRAPTORLabel(McRAPTORBaseLabel):
    def __init__(
        self,
        time: int,
        cost: int,
        stop: str,
        n_stops: int,
    ):
        super().__init__(time, stop)
        self.cost = cost
        self.n_stops = n_stops

    def strictly_dominates(self, other: McRAPTORLabel) -> bool:
        return self.arrival_time <= other.arrival_time and self.cost <= other.cost

    def update_along_trip(self, arrival_time: int, stop_id: str, trip_id: str):
        super().update_along_trip(arrival_time, stop_id, trip_id)
        # if self.n_stops == 0:
        #     self.cost += COST_SHORT_DISTANCE_TICKET_INCR
        # if self.n_stops == LENGTH_SHORT_DISTANCE_TICKET:
        #     self.cost -= COST_SHORT_DISTANCE_TICKET_INCR
        #     self.cost += COST_LONG_DISTANCE_TICKET_INCR
        # self.n_stops += 1

    def update_along_footpath(self, walking_time: int, stop_id: str):
        raise NotImplementedError("This label should not be updated along a footpath")

    def update_before_route_bag_merge(self, departure_time: int, stop_id: str):
        super().update_before_route_bag_merge(departure_time, stop_id)

    def update_before_stop_bag_merge(self, stop_id: str):
        pass
        # self.n_stops = 4  # artifically set to 4, so that if another public transport trip is taken, long distance ticket is used

    def to_intermediate_label(self, node_id: int) -> IntermediateLabel:
        return IntermediateLabel(
            values=[self.arrival_time, self.cost],
            hidden_values=[0, self.n_stops],
            path=[],
            osm_node_id=node_id,
        )


class McRAPTORLabelWithPath(McRAPTORLabel):
    STOP_PREFIX = "STOP_"
    TRIP_PREFIX = "TRIP_"

    def __init__(self, time: int, cost: int, stop: str, n_stops, path: list[int | str]):
        super().__init__(time, cost, stop, n_stops=n_stops)
        self.path = path

    def __str__(self):
        return f"McRAPTORLabelWithPath(time={self.arrival_time}, cost={self.cost}, n_stops={self.n_stops}, path={self.path})"

    def __repr__(self):
        return str(self)

    def update_along_trip(self, arrival_time: int, stop_id: str, trip_id: str):
        super().update_along_trip(arrival_time, stop_id, trip_id)
        trip_id = self.TRIP_PREFIX + trip_id
        if self.path[-1] != trip_id:
            self.path.append(trip_id)

    def update_before_route_bag_merge(self, departure_time: int, stop_id: str):
        super().update_before_route_bag_merge(departure_time, stop_id)
        self.path.append(self.STOP_PREFIX + stop_id)

    def update_before_stop_bag_merge(self, stop_id: str):
        super().update_before_stop_bag_merge(stop_id)
        self.path.append(self.STOP_PREFIX + stop_id)

    def to_intermediate_label(self, node_id: int) -> IntermediateLabel:
        intermediate_label = super().to_intermediate_label(node_id)
        intermediate_label.path = [
            convert_mc_raptor_path_element(path_element) for path_element in self.path
        ]
        return intermediate_label


def convert_mc_raptor_path_element(path_element: int | str) -> int | str:
    if isinstance(path_element, int):
        return path_element
    if path_element.startswith(McRAPTORLabelWithPath.STOP_PREFIX):
        return int(path_element[len(McRAPTORLabelWithPath.STOP_PREFIX) :])
    elif path_element.startswith(McRAPTORLabelWithPath.TRIP_PREFIX):
        return path_element[len(McRAPTORLabelWithPath.TRIP_PREFIX) :]
    else:
        raise ValueError(f"Unknown path element {path_element}")
