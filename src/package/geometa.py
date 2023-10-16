from shapely.geometry import Point, Polygon
import pickle
import geopandas as gpd
import pandas as pd
import folium

from package import cache


class GeoMeta:
    """
    GeoMeta incorporates general geospatial information, including the boundary of the area of consideration.
    """

    def __init__(self, boundary: Polygon):
        self.boundary = boundary

    def hash(self):
        return cache.hash_str(self.boundary.wkt)

    @staticmethod
    def load(path: str):
        with open(path, "rb") as f:
            loaded = pickle.load(f)
            if not isinstance(loaded, GeoMeta):
                raise ValueError(f"File at {path} does not contain a GeoMeta object.")
            return loaded

    def save(self, path: str):
        with open(path, "wb") as f:
            pickle.dump(self, f)

    def crop_gdf(
        self, locations: gpd.GeoDataFrame, buffer: float = 0
    ) -> gpd.GeoDataFrame:
        boundary = self.boundary
        if buffer > 0:
            boundary = boundary.buffer(buffer)

        locations = locations.loc[locations.geometry.within(boundary), :]

        return locations

    def crop_df(
        self, locations: pd.DataFrame, lat_col: str, lon_col: str, buffer: float = 0
    ) -> pd.DataFrame:
        boundary = self.boundary
        if buffer > 0:
            boundary = boundary.buffer(buffer)

        locations = locations.loc[
            locations.apply(
                lambda x: boundary.contains(Point(x[lon_col], x[lat_col])),
                axis=1,
            ),
            :,
        ]

        return locations

    def get_center_lat_lon(self) -> tuple[float, float]:
        lon, lat = self.boundary.centroid.coords[0]
        return lat, lon

    def add_to_folium_map(self, m: folium.Map) -> folium.Map:
        folium.GeoJson(self.boundary).add_to(m)
        return m
