from typing_extensions import Any
from rich import print
from rich.tree import Tree


def print_tree_from_any(data: Any, root_name: str = ":root:"):
    if isinstance(data, dict):
        tree = _build_tree_from_dict(data, parent=Tree(root_name))
    elif isinstance(data, list):
        tree = _build_tree_from_list(data, parent=Tree(root_name))
    else:
        raise ValueError(f"Cannot print tree from type {type(data)}")
    print(tree)


def _build_tree_from_dict(data, parent=None):
    if parent is None:
        parent = Tree(":root:")
    for key, value in data.items():
        if isinstance(value, dict):
            subtree = parent.add(f"{key}")
            _build_tree_from_dict(value, parent=subtree)
        elif isinstance(value, list):
            subtree = parent.add(f"{key}")
            _build_tree_from_list(value, parent=subtree)
        else:
            parent.add(f"{key}: {value}")

    return parent


def _build_tree_from_list(data, parent=None):
    if parent is None:
        parent = Tree(":root:")
    for item in data:
        if isinstance(item, dict):
            item_subtree = parent.add("")
            _build_tree_from_dict(item, parent=item_subtree)
        elif isinstance(item, list):
            item_subtree = parent.add("")
            _build_tree_from_list(item, parent=item_subtree)
        else:
            parent.add(str(item))

    return parent
