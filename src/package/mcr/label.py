from __future__ import annotations


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
