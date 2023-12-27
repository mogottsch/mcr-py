from typing import Callable, Optional
import pandas as pd
import numpy as np
from h3 import h3
from package import key
from multiprocessing import Pool
import folium
from branca.colormap import linear
from branca.colormap import LinearColormap


def add_h3_cell_id_to_df(df: pd.DataFrame, resolution: int) -> pd.DataFrame:
    """
    Add a column to the dataframe with the H3 cell ID for the given resolution.
    Expected columns are "lat" and "lon".
    """
    df["h3_cell_id"] = df.apply(
        lambda row: h3.geo_to_h3(row["lat"], row["lon"], resolution), axis=1
    )
    return df


def process_batch(df_batch, resolution):
    df_batch["h3_cell_id"] = df_batch.apply(
        lambda row: h3.geo_to_h3(row["lat"], row["lon"], resolution), axis=1
    )
    return df_batch


def add_h3_cell_id_to_df_with_batching(
    df: pd.DataFrame, resolution: int, n_batches: int
) -> pd.DataFrame:
    dfs = np.array_split(df, n_batches)

    with Pool(processes=key.DEFAULT_N_PROCESSES) as pool:
        results = pool.starmap(
            process_batch, [(df_batch, resolution) for df_batch in dfs]
        )

    return pd.concat(results, ignore_index=True)


def plot_h3_cells_on_folium(
    h3_cells: set[str] | dict[str, int],
    folium_map: folium.Map,
    reverse_color: bool = False,
    popup_callback: Optional[Callable] = None,
    color: str = "blue",
    maximum: Optional[int] = None,
    show_legend: bool = False,
    legend_color_map=None,
    legend_is_scaled: bool = False,
    legend_value_callback: Optional[Callable] = None,
    legend_caption: str = "",
) -> None:
    is_dict = isinstance(h3_cells, dict)
    maximum_value = (
        maximum if maximum is not None else (max(h3_cells.values()) if is_dict else 0)
    )

    if show_legend:
        colormap = legend_color_map or LinearColormap([(255, 255, 255, 0), color])
        if not legend_is_scaled:
            if legend_value_callback:
                maximum_value_legend = legend_value_callback(maximum_value)
            else:
                maximum_value_legend = maximum_value
            colormap = colormap.scale(0, maximum_value_legend)
        colormap.caption = legend_caption
        colormap.add_to(folium_map)

    for h3_cell in h3_cells:
        geo_boundary = list(h3.h3_to_geo_boundary(h3_cell))
        geo_boundary.append(geo_boundary[0])

        opacity = 0
        value = None
        if is_dict:
            value = h3_cells[h3_cell]
            opacity = value / maximum_value
            if reverse_color:
                opacity = 1 - opacity

        popup = None
        if popup_callback:
            if popup_callback.__code__.co_argcount == 1:
                popup = popup_callback(value)
            elif popup_callback.__code__.co_argcount == 2:
                popup = popup_callback(h3_cell, value)
        else:
            popup = (f"Value: {value}" if value else None,)

        folium.Polygon(
            locations=geo_boundary,
            color=color,
            weight=0.2,
            opacity=1,
            fill_color=color,
            fill_opacity=opacity,
            popup=popup,
        ).add_to(folium_map)
