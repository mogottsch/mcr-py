import pandas as pd
import folium

from package.tracer.tracer import TraceStart, TraceTrip, TraceFootpath, Trace
from package import strtime


def add_tracer_list_to_folium_map(
    tracers: list[Trace], folium_map: folium.Map, stops_df: pd.DataFrame
):
    stops_df = stops_df.copy().set_index("stop_id")
    circle_marker_kwargs = {
        "radius": 4,
        "color": "cyan",
        "fill": True,
        "fill_color": "cyan",
    }
    for tracer in tracers:
        if isinstance(tracer, TraceStart):
            folium.CircleMarker(
                location=stops_df.loc[tracer.start_stop_id, ["stop_lat", "stop_lon"]],
                popup=tracer.__str__(),
                **circle_marker_kwargs,
            ).add_to(folium_map)
        elif isinstance(tracer, TraceFootpath):
            start_stop = stops_df.loc[tracer.start_stop_id]
            end_stop = stops_df.loc[tracer.end_stop_id]
            folium.PolyLine(
                locations=[
                    (start_stop["stop_lat"], start_stop["stop_lon"]),
                    (end_stop["stop_lat"], end_stop["stop_lon"]),
                ],
                popup=tracer.__str__(),
                color="blue",
            ).add_to(folium_map)
            folium.CircleMarker(
                location=(end_stop["stop_lat"], end_stop["stop_lon"]),
                popup=f"{end_stop['stop_name']} ({tracer.end_stop_id}) duration:{strtime.seconds_to_str_time(tracer.walking_time)}",
                **circle_marker_kwargs,
            ).add_to(folium_map)
        elif isinstance(tracer, TraceTrip):
            start_stop = stops_df.loc[tracer.start_stop_id]
            end_stop = stops_df.loc[tracer.end_stop_id]
            folium.PolyLine(
                locations=[
                    (start_stop["stop_lat"], start_stop["stop_lon"]),
                    (end_stop["stop_lat"], end_stop["stop_lon"]),
                ],
                popup=tracer.__str__(),
                color="blue",
            ).add_to(folium_map)
            folium.CircleMarker(
                location=(end_stop["stop_lat"], end_stop["stop_lon"]),
                popup=f"{end_stop['stop_name']} ({tracer.end_stop_id}) @ {strtime.seconds_to_str_time(tracer.arrival_time)}",
                **circle_marker_kwargs,
            ).add_to(folium_map)
        else:
            raise ValueError(f"Unknown tracer type {type(tracer).__name__}")
