import pytest
from package.mcr.label import IntermediateLabel

from package.mcr.path import PathManager


# Setting up a fixture for the PathManager instance
@pytest.fixture
def pm():
    return PathManager()


def test_add_path(pm):
    path_id1 = pm._add_path([1, 2, 3])
    path_id2 = pm._add_path(["a", "b", "c"])

    assert pm.paths[0] == [1, 2, 3]
    assert pm.paths[1] == ["a", "b", "c"]
    assert path_id1 == 0
    assert path_id2 == 1


def test_extract_path_from_label(pm):
    label = IntermediateLabel([10, 20], [30, 40], [1, 2, 3, "a", "b"], 99)
    path_id = pm.extract_path_from_label(label)

    assert path_id == 0
    assert label.path == [0]
    assert pm.paths[0] == [1, 2, 3, "a", "b"]

    # Testing offset
    label2 = IntermediateLabel([10, 20], [30, 40], [1, 2, "x", "y", 3], 99)
    path_id2 = pm.extract_path_from_label(label2, 2)

    assert path_id2 == 1
    assert label2.path == [1, 2]
    assert pm.paths[1] == ["x", "y", 3]


def test_reconstruct_path_for_label(pm):
    label = IntermediateLabel([10, 20], [30, 40], [1, 2, 3, "a", "b"], 99)
    pm.extract_path_from_label(label)

    reconstructed_path = pm.reconstruct_path_for_label(label)
    assert reconstructed_path == [1, 2, 3, "a", "b"]

    label2 = IntermediateLabel([10, 20], [30, 40], [1, 2, 3, "a", "b"], 99)
    pm.extract_path_from_label(label2)
    label3 = IntermediateLabel([10, 20], [30, 40], [1, 2, "x", "y", 3], 99)
    pm.extract_path_from_label(label3, 2)

    label_combined = IntermediateLabel([10, 20], [30, 40], [0, 2], 99)
    reconstructed_combined_path = pm.reconstruct_path_for_label(label_combined)

    assert reconstructed_combined_path == [1, 2, 3, "a", "b", "x", "y"]


