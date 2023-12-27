from typing import Any, Dict, List, Optional, Union


PyBags = dict[int, list[PyLabel]]


class GraphCache:
    def __init__(self) -> None:
        ...

    def set_graph(self, raw_edges: List[Dict[str, Any]]) -> None:
        ...

    def set_node_weights(self, node_weights: Dict[int, List[int]]) -> None:
        ...

    def summary(self) -> None:
        ...

    def validate_node_id(self, node_id: int) -> None:
        ...

    def get_edge_weights(self, start_node_id: int, end_node_id: int) -> List[int]:
        ...


class PyLabel:
    values: List[int]
    hidden_values: List[int]
    path: List[int]
    node_id: int


def run_mlc(graph_cache: GraphCache, start_node_id: int) -> PyBags:
    ...


def run_mlc_with_node_and_time(
    graph_cache: GraphCache,
    start_node_id: int,
    time: int,
    disable_paths: Optional[bool] = None,
    update_label_func: Optional[str] = None,
    enable_limit: Optional[bool] = None,
) -> PyBags:
    ...


def run_mlc_with_bags(
    graph_cache: GraphCache,
    bags: Dict[int, List[Union[PyLabel, Any]]],
    update_label_func: Optional[str] = None,
    disable_paths: Optional[bool] = None,
    enable_limit: Optional[bool] = None,
) -> PyBags:
    ...
