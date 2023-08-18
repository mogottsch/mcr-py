from mcr_py import PyLabel
from package.mcr.label import IntermediateLabel


def convert_to_intermediate_bags(
    bags: dict[int, list[PyLabel]]
) -> dict[int, list[IntermediateLabel]]:
    python_bags = {
        node_id: [
            IntermediateLabel(
                label.values,
                label.hidden_values,
                label.path,
                label.node_id,
            )
            for label in bag
        ]
        for node_id, bag in bags.items()
    }
    return python_bags
