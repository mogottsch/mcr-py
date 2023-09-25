from enum import Enum
from typing import Any, Optional

from package.mcr.label import IntermediateLabel


PathPoints = list[int | str]


class PathType(Enum):
    WALKING = "walking"
    CYCLING_WALKING = "cycling_walking"
    PUBLIC_TRANSPORT = "public_transport"


class Path:
    def __init__(self, path_type: PathType, path: PathPoints, meta: Optional[dict[str, Any]] = None):
        self.path_type = path_type
        self.path = path
        self.meta = meta

    def __str__(self):
        return f"Path(path_type={self.path_type}, path={self.path}, meta={self.meta})"
    
    def __repr__(self):
        return str(self)


class GTFSPath:
    def __init__(self, start_stop_id: int, end_stop_id: int, trip_id: str, meta: Optional[dict[str, Any]] = None):
        self.start_stop_id = start_stop_id
        self.end_stop_id = end_stop_id
        self.trip_id = trip_id
        self.meta = meta


class PathManager:
    def __init__(self):
        self.paths: dict[int, Path] = {}
        self.path_id_counter = 0

    def __str__(self):
        return f"PathManager(path_id_counter={self.path_id_counter})"

    def __repr__(self):
        return str(self)

    def _add_path(self, path_type: PathType, path: PathPoints, meta: Optional[dict[str, Any]] = None) -> int:
        path_id = self.path_id_counter
        self.paths[path_id] = Path(path_type, path, meta=meta)
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

        meta = {
            "values": label.values,
            "hidden_values": label.hidden_values,
        }
        path_id = self._add_path(path_type, label_path, meta=meta)
        label.path.append(
            path_id
        )

        return path_id

    def reconstruct_and_translate_path_for_label(
        self, label: IntermediateLabel, translator_map: dict[PathType, dict[Any, Any]]
    ) -> list[Any]:
        translated_path: list[Any] = []
        for path_id in label.path:
            assert isinstance(path_id, int)
            path = self.paths[path_id]
            if path.path_type in [PathType.WALKING, PathType.CYCLING_WALKING]:
                translated_path.append(
                    Path(
                        path_type=path.path_type,
                        path=[translator_map[path.path_type][p] for p in path.path],
                        meta=path.meta,
                    )
                )
            elif path.path_type == PathType.PUBLIC_TRANSPORT:
                if len(path.path) != 3:
                    raise ValueError(
                        f"Expected path to have length 3, got {len(path.path)} instead. Path: {path.path}"
                    )
                translated_path.append(
                    GTFSPath(
                        start_stop_id=int(path.path[0]),
                        trip_id=str(path.path[1]),
                        end_stop_id=int(path.path[2]),
                        meta=path.meta,
                    )
                )
        return translated_path
