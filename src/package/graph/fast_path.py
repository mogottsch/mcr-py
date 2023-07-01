import geopandas as gpd
import mcr_py


def query_multiple_one_to_many(
    source_target_nodes_map: dict[int, list[int]],
    edges_gdf: gpd.GeoDataFrame,
) -> dict[int, dict[int, float]]:
    edges = edges_gdf[["u", "v", "length"]].to_dict(orient="records")
    return mcr_py.query_multiple_one_to_many(edges, source_target_nodes_map)
