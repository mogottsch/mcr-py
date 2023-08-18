from __future__ import annotations

from package.raptor.bag import BaseLabel as McRAPTORBaseLabel


class IntermediateLabel:
    def __init__(
        self,
        values: list[int],
        hidden_values: list[int],
        path: list[int | str],
        node_id: int,
    ):
        self.values = values
        self.hidden_values = hidden_values
        self.path = path
        self.node_id = node_id

    def __str__(self):
        return f"IntermediateLabel(values={self.values}, hidden_values={self.hidden_values}, path={self.path}, node_id={self.node_id})"

    def __repr__(self):
        return str(self)

    def copy_with_node_id(self, node_id: int) -> IntermediateLabel:
        return IntermediateLabel(
            values=self.values,
            hidden_values=self.hidden_values,
            path=self.path.copy(),
            node_id=node_id,
        )

    def to_mlc_label(self, new_node_id: int) -> IntermediateLabel:
        return IntermediateLabel(
            values=self.values + [0],
            hidden_values=[0],
            path=self.path,
            node_id=new_node_id,
        )

    def to_mc_raptor_label(self, stop_id: str) -> McRAPTORLabel:
        return McRAPTORLabel(
            time=self.values[0],
            stop=stop_id,
            path=self.path,
        )


class McRAPTORLabel(McRAPTORBaseLabel):
    def __init__(self, time: int, stop: str, path: list[int | str]):
        super().__init__(time, stop)
        self.path = path + [stop]

    def strictly_dominates(self, other: McRAPTORLabel) -> bool:
        return self.arrival_time < other.arrival_time

    def update_along_trip(self, arrival_time: int, stop_id: str, trip_id: str):
        super().update_along_trip(arrival_time, stop_id, trip_id)
        self.path.append(trip_id)

    def update_along_footpath(self, walking_time: int, stop_id: str):
        raise NotImplementedError("This label should not be updated along a footpath")

    def update_before_route_bag_merge(self, departure_time: int, stop_id: str):
        super().update_before_route_bag_merge(departure_time, stop_id)
        self.path.append(stop_id)
