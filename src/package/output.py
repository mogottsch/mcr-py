import os

import pandas as pd

from package import key, storage
from package.gtfs import archive
from package.tracer.tracer import (
    EnrichedTraceFootpath,
    EnrichedTraceStart,
    EnrichedTraceTrip,
    Trace,
    TraceFootpath,
    TraceStart,
    TraceTrip,
    TracerMap,
)


class TraceEnricher:
    def __init__(
        self, stops_df: pd.DataFrame, trips_df: pd.DataFrame, routes_df: pd.DataFrame
    ):
        self.stops_df = stops_df.set_index(key.STOP_ID_KEY)
        self.trips_df = trips_df.set_index(key.TRIP_ID_KEY)
        self.routes_df = routes_df.set_index(key.ROUTE_ID_KEY)

    def enrich_traces(self, traces: list[Trace]) -> list[Trace]:
        new_traces: list[Trace] = []
        for trace in traces:
            new_traces.append(self.enrich_trace(trace))

        return new_traces

    def enrich_trace(self, trace: Trace) -> Trace:
        if isinstance(trace, TraceStart):
            return self.enrich_trace_start(trace)
        elif isinstance(trace, TraceTrip):
            return self.enrich_trace_trip(trace)
        elif isinstance(trace, TraceFootpath):
            return self.enrich_trace_footpath(trace)

        raise ValueError(f"Unknown trace type: {type(trace).__name__}")

    def enrich_trace_start(self, trace: TraceStart) -> EnrichedTraceStart:
        stop_id = trace.start_stop_id
        stop = self.stops_df.loc[stop_id]

        return EnrichedTraceStart(trace, stop[key.STOP_NAME_KEY])

    def enrich_trace_trip(self, trace: TraceTrip) -> EnrichedTraceTrip:
        start_stop_id, end_stop_id = trace.start_stop_id, trace.end_stop_id
        trip_id = trace.trip_id

        start_stop = self.stops_df.loc[start_stop_id]
        end_stop = self.stops_df.loc[end_stop_id]
        trip = self.trips_df.loc[trip_id]

        route_id = trip[key.ROUTE_ID_KEY]
        route = self.routes_df.loc[route_id]

        return EnrichedTraceTrip(
            trace,
            f"{route[key.ROUTE_SHORT_NAME_KEY]} {trip[key.TRIP_HEADSIGN_KEY]}",
            start_stop[key.STOP_NAME_KEY],
            end_stop[key.STOP_NAME_KEY],
        )

    def enrich_trace_footpath(self, trace: TraceFootpath) -> TraceFootpath:
        start_stop_id, end_stop_id = trace.start_stop_id, trace.end_stop_id
        start_stop = self.stops_df.loc[start_stop_id]
        end_stop = self.stops_df.loc[end_stop_id]

        return EnrichedTraceFootpath(
            trace,
            start_stop[key.STOP_NAME_KEY],
            end_stop[key.STOP_NAME_KEY],
        )


def enrich_raptor_trace_results(results_dir_path: str, gtfs_dir_path: str):
    tracer_map_file = storage.read_any_dict(
        os.path.join(results_dir_path, key.RAPTOR_TRACE_FILE_NAME)
    )
    tracer_map: TracerMap = tracer_map_file[key.TRACER_MAP_KEY]

    dfs = archive.read_dfs(gtfs_dir_path)

    stops_df, trips_df, routes_df = (
        dfs[key.STOPS_KEY],
        dfs[key.TRIPS_KEY],
        dfs[key.ROUTES_KEY],
    )

    trace_enricher = TraceEnricher(stops_df, trips_df, routes_df)

    for stop_id, tracers in tracer_map.tracers.items():
        tracer_map.tracers[stop_id] = trace_enricher.enrich_traces(tracers)

    return tracer_map
