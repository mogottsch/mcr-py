from scipy.spatial import cKDTree
import numpy as np
import pandas as pd
from h3 import h3


class H3OSMLocationMapping:
    def __init__(self, h3_cell: str, osm_node_id: str, distance: float):
        self.h3_cell = h3_cell
        self.osm_node_id = osm_node_id
        self.distance = distance

    def to_dict(self) -> dict:
        return {
            "h3_cell": self.h3_cell,
            "osm_node_id": self.osm_node_id,
            "distance": self.distance,
        }

    def __str__(self) -> str:
        return str(self.to_dict())

    def __repr__(self) -> str:
        return str(self)


def get_location_mappings_for_cells(
    h3_cells: list[str], osm_nodes_df: pd.DataFrame, max_tries: int = 2
) -> tuple[list[H3OSMLocationMapping], list[str]]:
    """
    For each H3 cell, find the closest OSM node and return the mapping.
    It also checks whether the closest node is actually in the H3 cell.
    If not, it will try to find the next closest node.

    :param h3_cells: List of H3 cells
    :param osm_nodes_df: DataFrame with OSM nodes
    :param max_tries: Maximum number of tries to find a node in the H3 cell
    """

    location_mappings: list[H3OSMLocationMapping] = []

    osm_nodes_array = osm_nodes_df[["lat", "lon"]].to_numpy()
    kdtree = cKDTree(osm_nodes_array)

    invalid_h3_cells: list[str] = []

    cell_centers = [h3.h3_to_geo(h3_cell) for h3_cell in h3_cells]
    all_distances, all_indices = kdtree.query(cell_centers, k=max_tries)

    resolution = h3.h3_get_resolution(h3_cells[0])

    for h3_cell, (distances, indices) in zip(h3_cells, zip(all_distances, all_indices)):
        tries = 0
        distance, index = distances[tries], indices[tries]
        while tries < max_tries:
            distance, index = distances[tries], indices[tries]
            h3_cell_of_closest_node = h3.geo_to_h3(
                *osm_nodes_df.iloc[index][["lat", "lon"]].to_numpy(),
                resolution=resolution,
            )

            if h3_cell_of_closest_node == h3_cell:
                break

            tries += 1
        if tries == max_tries:
            invalid_h3_cells.append(h3_cell)
            print(f"WARN: None of the closest nodes are inside the H3 cell {h3_cell}")
            # continue
        node = osm_nodes_df.iloc[index]
        location_mappings.append(
            H3OSMLocationMapping(h3_cell, node.id, degree_distance_to_meters(distance))
        )

    return location_mappings, invalid_h3_cells


def degree_distance_to_meters(distance: float) -> float:
    return distance * 111111
