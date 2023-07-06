import pytest

from package.tracer.tracer import TracerMap, TraceStart, TraceTrip, TraceFootpath


@pytest.fixture
def tracer_map() -> TracerMap:
    return TracerMap({"stop1", "stop2", "stop3"})


def test_tracer_map_add_trace_start(tracer_map: TracerMap) -> None:
    trace_start = TraceStart("stop1", 0)
    tracer_map.add(trace_start)
    assert tracer_map["stop1"] == [trace_start]


def test_tracer_map_add_trace_trip(tracer_map: TracerMap) -> None:
    trace_start = TraceStart("stop1", 0)
    trace_trip = TraceTrip("stop1", 10, "stop2", 20, "trip1")
    tracer_map.add(trace_start)
    tracer_map.add(trace_trip)
    assert tracer_map["stop1"] == [trace_start]
    assert tracer_map["stop2"] == [trace_start, trace_trip]


def test_tracer_map_add_trace_footpath(tracer_map: TracerMap) -> None:
    trace_start = TraceStart("stop1", 0)
    trace_trip = TraceTrip("stop1", 10, "stop2", 20, "trip1")
    trace_footpath = TraceFootpath("stop2", "stop3", 30)
    tracer_map.add(trace_start)
    tracer_map.add(trace_trip)
    tracer_map.add(trace_footpath)
    assert tracer_map["stop1"] == [trace_start]
    assert tracer_map["stop2"] == [trace_start, trace_trip]
    assert tracer_map["stop3"] == [trace_start, trace_trip, trace_footpath]


def test_tracer_map_add_multiple_tracers(tracer_map: TracerMap) -> None:
    trace_start1 = TraceStart("stop1", 0)
    trace_start2 = TraceStart("stop2", 0)
    trace_trip = TraceTrip("stop1", 10, "stop2", 20, "trip1")
    tracer_map.add(trace_start1)
    tracer_map.add(trace_start2)
    tracer_map.add(trace_trip)
    assert tracer_map["stop1"] == [trace_start1]
    assert tracer_map["stop2"] == [trace_start1, trace_trip]
