import pyrosm
import geopandas as gpd
import networkx as nx
import osmnx as ox


def create_nx_graph(
    osm: pyrosm.OSM, nodes: gpd.GeoDataFrame, edges: gpd.GeoDataFrame
) -> nx.Graph:
    return osm.to_graph(nodes, edges, graph_type="networkx", network_type="walking")  # type: ignore


def add_nearest_node_to_stops(
    stops_df: gpd.GeoDataFrame, nx_graph: nx.Graph
) -> gpd.GeoDataFrame:
    nodes, dists = ox.nearest_nodes(
        nx_graph,
        stops_df.stop_lon.astype(float),  # TODO: improve this
        stops_df.stop_lat.astype(float),
        return_dist=True,
    )  # type: ignore
    stops_df["nearest_node"] = nodes
    stops_df["nearest_node_dist"] = dists
    return stops_df
