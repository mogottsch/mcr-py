import pyrosm
import geopandas as gpd
import networkx as nx
import osmnx as ox
from package.logger import rlog


def create_nx_graph(
    osm: pyrosm.OSM, nodes: gpd.GeoDataFrame, edges: gpd.GeoDataFrame, network_type: str
) -> nx.Graph:
    graph: nx.Graph = osm.to_graph(nodes, edges, graph_type="networkx", network_type=network_type)  # type: ignore

    return graph


def crop_graph_to_largest_component(
    graph: nx.Graph, nodes: gpd.GeoDataFrame, edges: gpd.GeoDataFrame
) -> tuple[nx.Graph, gpd.GeoDataFrame, gpd.GeoDataFrame]:
    weakly_connected_components = nx.weakly_connected_components(graph)
    largest_component = max(weakly_connected_components, key=len)

    graph = graph.subgraph(largest_component).copy()

    n_nodes_before, n_edges_before = len(nodes), len(edges)
    nodes = nodes[nodes["id"].isin(graph.nodes)]  # type: ignore
    edges = edges[edges["u"].isin(graph.nodes) & edges["v"].isin(graph.nodes)]  # type: ignore
    rlog.debug(
        f"Removed {n_nodes_before - len(nodes)} nodes and "
        + f" {n_edges_before - len(edges)} edges from OSM network to ensure"
        + f" connectivity ({(n_nodes_before-len(nodes))/n_nodes_before*100:.2f}%)"
    )
    return graph, nodes, edges


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
