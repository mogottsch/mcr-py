from typing import Callable
from mcr_py import PyLabel
from package.mcr.label import IntermediateLabel, McRAPTORLabel
from package.raptor.bag import Bag


# key is osm_node_id, value is list of labels
IntermediateBags = dict[int, list[IntermediateLabel]]


def convert_mlc_bags_to_intermediate_bags(
    bags: dict[int, list[PyLabel]],
    translate_node_id: Callable[[int], int],
    add_zero_weight_to_values: bool = False,
) -> IntermediateBags:
    intermediate_bags = {
        translate_node_id(node_id): [
            IntermediateLabel(
                label.values + ([0] if add_zero_weight_to_values else []),
                label.hidden_values,
                label.path,
                translate_node_id(node_id),
            )
            for label in bag
        ]
        for node_id, bag in bags.items()
    }
    return intermediate_bags


def convert_mc_raptor_bags_to_intermediate_bags(
    bags: dict[int, Bag],
    min_path_length: int,
) -> IntermediateBags:
    intermediate_bags: dict[int, list[IntermediateLabel]] = {}
    for node_id, bag in bags.items():
        intermediate_bags[int(node_id)] = []
        for label in bag:  # type: ignore
            if not isinstance(label, McRAPTORLabel):
                raise ValueError(
                    f"Expected McRAPTORLabel, got {str(type(label))} instead"
                )

            label: McRAPTORLabel = label
            if len(label.path) < min_path_length:
                continue

            intermediate_bags[int(node_id)].append(
                label.to_intermediate_label(int(node_id))
            )

    # remove empty bags
    intermediate_bags = {
        node_id: bag for node_id, bag in intermediate_bags.items() if len(bag) > 0
    }

    return intermediate_bags
