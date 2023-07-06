import shutil
from pandas.compat import os

from command.raptor import raptor
from package import storage, strtime
from package.key import TRACER_MAP_KEY
from package.output import enrich_raptor_trace_results
from package.tracer.tracer import (
    EnrichedTraceFootpath,
    EnrichedTraceStart,
    EnrichedTraceTrip,
)


NESSELRODE_STR_STOP_ID = "818"
EHRENFELD_BF_STOP_ID = "835"
AMSTERDAMER_STR_STOP_ID = "317"
VENLOER_STR_STOP_ID = "251"


def test_raptor(testdata_path: str):
    output_dir = os.path.join(testdata_path, "output")

    if os.path.exists(output_dir):
        shutil.rmtree(output_dir)

    raptor(
        footpaths=os.path.join(testdata_path, "footpaths.pkl"),
        structs=os.path.join(testdata_path, "structs.pkl"),
        start_stop_id=NESSELRODE_STR_STOP_ID,
        start_time="15:00:00",
        output_dir=output_dir,
        max_transfers=10,
    )

    tracer_map = enrich_raptor_trace_results(
        output_dir,
        os.path.join(testdata_path, "gtfs.zip"),
    )

    arrival_times = storage.read_df(
        os.path.join(output_dir, "arrival_times.csv")
    ).set_index("stop_id")

    # all stops are reachable
    assert (arrival_times.arrival_time == "--:--:--").sum() == 0
    assert arrival_times.loc[EHRENFELD_BF_STOP_ID, "arrival_time"] == "15:33:27"

    tracers = tracer_map.tracers[EHRENFELD_BF_STOP_ID]
    assert len(tracers) == 4
    start_trace = tracers[0]
    t16_trace = tracers[1]
    t13_trace = tracers[2]
    footpath_trace = tracers[3]

    assert isinstance(
        start_trace, EnrichedTraceStart
    ), f"type: {type(start_trace).__name__}"
    assert start_trace.start_stop_id == NESSELRODE_STR_STOP_ID
    assert start_trace.start_time == strtime.str_time_to_seconds("15:00:00")

    assert isinstance(t16_trace, EnrichedTraceTrip)
    assert t16_trace.start_stop_id == NESSELRODE_STR_STOP_ID
    assert t16_trace.end_stop_id == AMSTERDAMER_STR_STOP_ID
    assert t16_trace.departure_time == strtime.str_time_to_seconds("15:08:00")
    assert t16_trace.arrival_time == strtime.str_time_to_seconds("15:09:00")

    assert isinstance(t13_trace, EnrichedTraceTrip)
    assert t13_trace.start_stop_id == AMSTERDAMER_STR_STOP_ID
    assert t13_trace.end_stop_id == VENLOER_STR_STOP_ID
    assert t13_trace.departure_time == strtime.str_time_to_seconds("15:20:00")
    assert t13_trace.arrival_time == strtime.str_time_to_seconds("15:31:00")

    assert isinstance(footpath_trace, EnrichedTraceFootpath)
    assert footpath_trace.start_stop_id == VENLOER_STR_STOP_ID
    assert footpath_trace.end_stop_id == EHRENFELD_BF_STOP_ID
    assert footpath_trace.walking_time == strtime.str_time_to_seconds("00:02:27")
