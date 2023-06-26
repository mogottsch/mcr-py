import folium
import pandas as pd

from package import strtime


class Trace:
    pass

    def __repr__(self):
        return self.__str__()


class MovingTrace(Trace):
    def __init__(self, start_stop_id: str, end_stop_id: str):
        self.start_stop_id = start_stop_id
        self.end_stop_id = end_stop_id


class TracerMap:
    def __init__(self, stop_ids: set[str]):
        self.tracers = {stop_id: [] for stop_id in stop_ids}

    def __str__(self):
        return "\n".join(
            [f"{stop_id}: {tracers}" for stop_id, tracers in self.tracers.items()]
        )

    def __getitem__(self, stop_id: str):
        return self.tracers[stop_id]

    def add(self, tracer: Trace):
        if isinstance(tracer, MovingTrace):
            end_stop_id = tracer.end_stop_id
            start_stop_id = tracer.start_stop_id

            previous_tracers = self.tracers[start_stop_id]
            new_tracers = previous_tracers + [tracer]

            self.tracers[end_stop_id] = new_tracers
        elif isinstance(tracer, TraceStart):
            self.tracers[tracer.start_stop_id] = [tracer]
        else:
            raise ValueError(f"Unknown tracer type: {type(tracer)}")


class TraceStart(Trace):
    def __init__(self, start_stop_id: str, start_time: int):
        self.start_stop_id = start_stop_id
        self.start_time = start_time

    def __str__(self):
        return f"Start at {self.start_stop_id} at {strtime.seconds_to_str_time(self.start_time)}"


class TraceTrip(MovingTrace):
    def __init__(
        self,
        start_stop_id: str,
        departure_time: int,
        end_stop_id: str,
        arrival_time: int,
        trip_id: str,
    ):
        super().__init__(start_stop_id, end_stop_id)
        self.departure_time = departure_time
        self.arrival_time = arrival_time
        self.trip_id = trip_id

    def __str__(self):
        return (
            f"Trip {self.trip_id} from {self.start_stop_id}@"
            + f"{strtime.seconds_to_str_time(self.departure_time)} to "
            + f"{self.end_stop_id}@{strtime.seconds_to_str_time(self.arrival_time)}"
        )


class TraceFootpath(MovingTrace):
    def __init__(self, start_stop_id: str, end_stop_id: str, walking_time: int):
        super().__init__(start_stop_id, end_stop_id)
        self.walking_time = walking_time

    def __str__(self):
        return f"Walk from {self.start_stop_id} to {self.end_stop_id} in {strtime.seconds_to_str_time(self.walking_time)}"


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
            raise ValueError(f"Unknown tracer type {type(tracer)}")
