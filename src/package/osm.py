from typing import Tuple
from typing_extensions import Annotated

import pyrosm
import geopandas as gpd
from shapely.geometry import MultiPoint

from package.logger import llog
from package import console


def list_available(selector: str):
    root_name = "Available OSM data"
    if selector != "":
        root_name += f" for {selector}"
    console.print_tree_from_any(get_available(selector), root_name=root_name)


def get_available(
    selector: Annotated[str, "Selector in dot notation, e.g. '.regions.africa'"]
):
    data = pyrosm.data.available  # type: ignore
    if selector == "":
        return data

    # remove first . if present
    if len(selector) > 0 and selector[0] == ".":
        selector = selector[1:]
    keys = selector.split(".")
    for key in keys:
        if key not in data:
            raise ValueError(f"Key '{key}' not found in OSM data.")
        data = data[key]
    return data


def new_osm_reader(path: str) -> pyrosm.OSM:
    return pyrosm.OSM(path)


def read_network(osm_reader: pyrosm.OSM) -> Tuple[gpd.GeoDataFrame, gpd.GeoDataFrame]:
    network = osm_reader.get_network(nodes=True, network_type="walking")
    if network is None:
        raise Exception("No walking network found in OSM data.")
    nodes, edges = network
    return nodes, edges  # type: ignore


def crop_to_stops(
    nodes: gpd.GeoDataFrame, edges: gpd.GeoDataFrame, stops_df: gpd.GeoDataFrame
):
    combined_geometry = MultiPoint(stops_df.geometry.tolist())
    convex_hull = combined_geometry.convex_hull
    zone_of_interest = convex_hull.buffer(0.01)

    n_nodes_before = len(nodes)
    n_edges_before = len(edges)

    nodes = nodes.loc[nodes.geometry.within(zone_of_interest), :]
    edges = edges.loc[edges.u.isin(nodes.id) & edges.v.isin(nodes.id), :]
    llog.info(
        f"{len(nodes)}/{n_nodes_before} ({len(nodes)/n_nodes_before*100:.2f}%) nodes remaining"
    )
    llog.info(
        f"{len(edges)}/{n_edges_before} ({len(edges)/n_edges_before*100:.2f}%) edges remaining"
    )

    return nodes, edges
