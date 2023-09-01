import pytest
from package.mcr.label import IntermediateLabel

from package.mcr.path import PathManager, PathType, Path, GTFSPath

import pytest


@pytest.fixture
def path_manager() -> PathManager:
    return PathManager()


def test_path_manager_initialization(path_manager: PathManager) -> None:
    assert path_manager.path_id_counter == 0
    assert path_manager.paths == {}


def test_path_manager_add_path(path_manager: PathManager) -> None:
    path_id = path_manager._add_path(PathType.WALKING, [1, 2, 3])
    assert path_id == 0
    assert path_manager.path_id_counter == 1
    assert path_manager.paths[0].path_type == PathType.WALKING
    assert path_manager.paths[0].path == [1, 2, 3]


def test_extract_all_paths_from_bags(path_manager: PathManager) -> None:
    il1 = IntermediateLabel([1, 1], [1, 1], [1, "a"], 1)
    il2 = IntermediateLabel([2, 2], [2, 2], [2, "b"], 2)
    bags = {1: [il1], 2: [il2]}

    path_manager.extract_all_paths_from_bags(bags, PathType.WALKING)

    assert path_manager.paths[0].path_type == PathType.WALKING
    assert path_manager.paths[0].path == [1, "a"]
    assert path_manager.paths[1].path_type == PathType.WALKING
    assert path_manager.paths[1].path == [2, "b"]


def test_extract_path_from_label(path_manager: PathManager) -> None:
    il = IntermediateLabel([1, 1], [1, 1], [1, "a"], 1)

    path_id = path_manager.extract_path_from_label(il, PathType.WALKING)

    assert path_id == 0
    assert il.path == [0]
    assert path_manager.paths[0].path_type == PathType.WALKING
    assert path_manager.paths[0].path == [1, "a"]


def test_reconstruct_and_translate_path_for_label(path_manager: PathManager) -> None:
    path_manager._add_path(PathType.WALKING, [1, "a"])
    path_manager._add_path(PathType.PUBLIC_TRANSPORT, [1, "trip_1", 2])

    il = IntermediateLabel([1, 1], [1, 1], [0, 1], 1)
    translator_map = {
        PathType.WALKING: {1: "one", "a": "A"},
        PathType.PUBLIC_TRANSPORT: {},
    }

    translated_path = path_manager.reconstruct_and_translate_path_for_label(
        il, translator_map
    )

    assert len(translated_path) == 2
    assert isinstance(translated_path[0], Path)
    assert translated_path[0].path == ["one", "A"]
    assert isinstance(translated_path[1], GTFSPath)
    assert translated_path[1].start_stop_id == 1
    assert translated_path[1].trip_id == "trip_1"
    assert translated_path[1].end_stop_id == 2
