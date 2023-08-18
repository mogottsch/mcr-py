import shutil
from pandas.compat import os
from package.raptor.example_labels import ArrivalTimeLabel

from package.raptor.mcraptor import McRaptor
from package import storage, strtime
from package.tracer.tracer import (
    TraceFootpath,
    TraceStart,
    TraceTrip,
)


NESSELRODE_STR_STOP_ID = "818"
EHRENFELD_BF_STOP_ID = "835"
AMSTERDAMER_STR_STOP_ID = "317"
VENLOER_STR_STOP_ID = "251"


def test_mcraptor(testdata_path: str):
    output_dir = os.path.join(testdata_path, "output")

    if os.path.exists(output_dir):
        shutil.rmtree(output_dir)

    footpaths_dict = storage.read_any_dict(os.path.join(testdata_path, "footpaths.pkl"))
    footpaths_dict = footpaths_dict["footpaths"]

    structs_dict = storage.read_any_dict(os.path.join(testdata_path, "structs.pkl"))

    mc_raptor = McRaptor(
        structs_dict, footpaths_dict, 10, 180, {}, {}, ArrivalTimeLabel
    )
    bags: dict[str, dict] = mc_raptor.run(NESSELRODE_STR_STOP_ID, "", "15:00:00")

    assert any(len(b) == 1 for b in bags.values())

    tracers = bags[EHRENFELD_BF_STOP_ID][0]["traces"]
    assert len(tracers) == 4
    start_trace = tracers[0]
    t16_trace = tracers[1]
    t13_trace = tracers[2]
    footpath_trace = tracers[3]

    assert isinstance(start_trace, TraceStart), f"type: {type(start_trace).__name__}"
    assert start_trace.start_stop_id == NESSELRODE_STR_STOP_ID
    assert start_trace.start_time == strtime.str_time_to_seconds("15:00:00")

    assert isinstance(t16_trace, TraceTrip)
    assert t16_trace.start_stop_id == NESSELRODE_STR_STOP_ID
    assert t16_trace.end_stop_id == AMSTERDAMER_STR_STOP_ID
    assert t16_trace.departure_time == strtime.str_time_to_seconds("15:08:00")
    assert t16_trace.arrival_time == strtime.str_time_to_seconds("15:09:00")

    assert isinstance(t13_trace, TraceTrip)
    assert t13_trace.start_stop_id == AMSTERDAMER_STR_STOP_ID
    assert t13_trace.end_stop_id == VENLOER_STR_STOP_ID
    assert t13_trace.departure_time == strtime.str_time_to_seconds("15:13:00")
    assert t13_trace.arrival_time == strtime.str_time_to_seconds("15:25:00")

    assert isinstance(footpath_trace, TraceFootpath)
    assert footpath_trace.start_stop_id == VENLOER_STR_STOP_ID
    assert footpath_trace.end_stop_id == EHRENFELD_BF_STOP_ID
    assert footpath_trace.walking_time == strtime.str_time_to_seconds("00:02:27")
