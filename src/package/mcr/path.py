from enum import Enum
from typing import Any

from package.mcr.label import IntermediateLabel


PathPoints = list[int | str]


class PathType(Enum):
    WALKING = "walking"
    CYCLING_WALKING = "cycling_walking"
    PUBLIC_TRANSPORT = "public_transport"


class Path:
    def __init__(self, path_type: PathType, path: PathPoints):
        self.path_type = path_type
        self.path = path


class PathManager:
    def __init__(self):
        self.paths: dict[int, Path] = {}
        self.path_id_counter = 0

    def __str__(self):
        return f"PathManager(path_id_counter={self.path_id_counter})"

    def __repr__(self):
        return str(self)

    def _add_path(self, path_type: PathType, path: PathPoints) -> int:
        path_id = self.path_id_counter
        self.paths[path_id] = Path(path_type, path)
        self.path_id_counter += 1
        return path_id

    def extract_all_paths_from_bags(
        self,
        bags: dict[int, list[IntermediateLabel]],
        path_type: PathType,
        path_index_offset: int = 0,
    ):
        for bag in bags.values():
            for label in bag:
                self.extract_path_from_label(
                    label, path_type, path_index_offset=path_index_offset
                )

    def extract_path_from_label(
        self, label: IntermediateLabel, path_type: PathType, path_index_offset: int = 0
    ) -> int:
        label_path = label.path[path_index_offset:]
        label.path = label.path[:path_index_offset]

        path_id = self._add_path(path_type, label_path)
        label.path.append(
            # -path_id  # we use negative path ids to differentiate between path ids and node ids
            path_id  # yolo
        )

        return path_id

    def reconstruct_and_translate_path_for_label(
        self, label: IntermediateLabel, translator_map: dict[PathType, dict[Any, Any]]
    ) -> list[Path]:
        translated_path: list[Path] = []
        for path_id in label.path:
            assert isinstance(path_id, int)
            path = self.paths[path_id]
            translated_path.append(
                Path(
                    path_type=path.path_type,
                    path=[translator_map[path.path_type][p] for p in path.path],
                )
            )
        return translated_path
