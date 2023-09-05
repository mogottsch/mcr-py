from h3 import h3
import folium


def get_h3_cells_for_nodes(nodes: list[dict], resolution: int) -> set[str]:
    h3_cells: set[str] = set()

    for node in nodes:
        lat, lon = node["lat"], node["lon"]
        h3_cell = h3.geo_to_h3(lat, lon, resolution)
        h3_cells.add(h3_cell)

    return h3_cells


def get_h3_cells_for_bbox(
    min_lat: float, min_lon: float, max_lat: float, max_lon: float, resolution: int
) -> set[str]:
    h3_cells: set[str] = set()

    # Get the edge length in meters for the H3 resolution
    edge_length_m = h3.edge_length(resolution, unit="m")

    # Approximate step size in degrees, assuming 111,111 meters per degree
    # This is a rough approximation and works best near the equator.
    # The closer to the poles you get, the less accurate this becomes.
    step_size = edge_length_m / 111111.0

    lat = min_lat
    while lat <= max_lat:
        lon = min_lon
        while lon <= max_lon:
            h3_cell = h3.geo_to_h3(lat, lon, resolution)
            h3_cells.add(h3_cell)
            lon += step_size
        lat += step_size

    return h3_cells


def plot_h3_cells_on_folium(
    h3_cells: set[str] | dict[str, int], folium_map: folium.Map
) -> None:
    is_dict = isinstance(h3_cells, dict)
    maximum = max(h3_cells.values()) if is_dict else 0
    for h3_cell in h3_cells:
        geo_boundary = list(h3.h3_to_geo_boundary(h3_cell))
        geo_boundary.append(geo_boundary[0])

        opacity = 0.2
        count = None
        if is_dict:
            count = h3_cells[h3_cell]
            opacity = count / maximum

        folium.Polygon(
            locations=geo_boundary,
            color="blue",
            weight=2.5,
            opacity=1,
            fill_color="blue",
            fill_opacity=opacity,
            popup=f"Count: {count}" if count else None,
        ).add_to(folium_map)
