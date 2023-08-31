from __future__ import annotations

from package.raptor.bag import BaseLabel as McRAPTORBaseLabel


class IntermediateLabel:
    def __init__(
        self,
        values: list[int],
        hidden_values: list[int],
        path: list[int | str],
        node_id: int,  # int for osm nodes, str for gtfs stops
    ):
        self.values = values
        self.hidden_values = hidden_values
        self.path = path
        self.node_id = node_id

    def __str__(self):
        return f"IntermediateLabel(values={self.values}, hidden_values={self.hidden_values}, path={self.path}, node_id={self.node_id})"

    def __repr__(self):
        return str(self)

    def strictly_dominates(self, other: IntermediateLabel) -> bool:
        assert len(self.values) == len(other.values)
        return all([self.values[i] <= other.values[i] for i in range(len(self.values))])

    def copy_with_node_id(self, node_id: int) -> IntermediateLabel:
        return IntermediateLabel(
            values=self.values,
            hidden_values=self.hidden_values,
            path=self.path.copy(),
            node_id=node_id,
        )

    def to_mlc_label(self, new_node_id: int) -> IntermediateLabel:
        return IntermediateLabel(
            values=self.values,
            hidden_values=[0],
            path=self.path,
            node_id=new_node_id,
        )

    def to_mc_raptor_label(
        self, stop_id: str, null_cost: bool = False
    ) -> McRAPTORLabel:
        return McRAPTORLabel(
            time=self.values[0],
            cost=0 if null_cost else self.values[1],
            stop=stop_id,
            path=self.path,
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
    STOP_PREFIX = "STOP_"
    TRIP_PREFIX = "TRIP_"

    def __init__(self, time: int, cost: int, stop: str, path: list[int | str]):
        super().__init__(time, stop)
        self.path = path
        self.cost = cost

    def strictly_dominates(self, other: McRAPTORLabel) -> bool:
        return self.arrival_time <= other.arrival_time and self.cost <= other.cost

    def update_along_trip(self, arrival_time: int, stop_id: str, trip_id: str):
        super().update_along_trip(arrival_time, stop_id, trip_id)
        trip_id = self.TRIP_PREFIX + trip_id
        if self.path[-1] != trip_id:
            self.path.append(trip_id)

    def update_along_footpath(self, walking_time: int, stop_id: str):
        raise NotImplementedError("This label should not be updated along a footpath")

    def update_before_route_bag_merge(self, departure_time: int, stop_id: str):
        super().update_before_route_bag_merge(departure_time, stop_id)
        self.path.append(self.STOP_PREFIX + stop_id)

    def update_before_stop_bag_merge(self, stop_id: str):
        self.path.append(self.STOP_PREFIX + stop_id)

    def to_intermediate_label(self, node_id: int) -> IntermediateLabel:
        return IntermediateLabel(
            values=[self.arrival_time, self.cost],
            hidden_values=[],
            path=[
                convert_mc_raptor_path_element(path_element)
                for path_element in self.path
            ],
            node_id=node_id,
        )


def convert_mc_raptor_path_element(path_element: int | str) -> int | str:
    if isinstance(path_element, int):
        return path_element
    if path_element.startswith(McRAPTORLabel.STOP_PREFIX):
        return int(path_element[len(McRAPTORLabel.STOP_PREFIX) :])
    elif path_element.startswith(McRAPTORLabel.TRIP_PREFIX):
        return path_element[len(McRAPTORLabel.TRIP_PREFIX) :]
    else:
        raise ValueError(f"Unknown path element {path_element}")
