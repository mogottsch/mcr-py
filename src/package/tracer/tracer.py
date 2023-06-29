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
        self.tracers: dict[str, list[Trace]] = {stop_id: [] for stop_id in stop_ids}

    def __str__(self):
        return "\n".join(
            [f"{stop_id}: {tracers}" for stop_id, tracers in self.tracers.items()]
        )

    def __repr__(self):
        return self.__str__()

    def __getitem__(self, stop_id: str):
        return self.tracers[stop_id]

    def add(self, tracer: Trace):
        if isinstance(tracer, MovingTrace):
            end_stop_id = tracer.end_stop_id
            start_stop_id = tracer.start_stop_id

            previous_tracers = self.tracers[start_stop_id]

            if len(previous_tracers) == 0 or not isinstance(
                previous_tracers[0], TraceStart
            ):
                raise ValueError(
                    f"The first tracer for stop {start_stop_id} must be a TraceStart"
                )

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


class EnrichedTraceStart(TraceStart):
    def __init__(self, trace_start: TraceStart, start_stop_name: str):
        super().__init__(trace_start.start_stop_id, trace_start.start_time)
        self.start_stop_name = start_stop_name

    def __str__(self):
        return f"Start at {self.start_stop_name} ({self.start_stop_id}) at {strtime.seconds_to_str_time(self.start_time)}"


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


class EnrichedTraceTrip(TraceTrip):
    def __init__(
        self,
        trace_trip: TraceTrip,
        trip_name: str,
        start_stop_name: str,
        end_stop_name: str,
    ):
        super().__init__(
            trace_trip.start_stop_id,
            trace_trip.departure_time,
            trace_trip.end_stop_id,
            trace_trip.arrival_time,
            trace_trip.trip_id,
        )
        self.trip_name = trip_name
        self.start_stop_name = start_stop_name
        self.end_stop_name = end_stop_name

    def __str__(self):
        return (
            f"Trip {self.trip_name} from {self.start_stop_name}@{strtime.seconds_to_str_time(self.departure_time)} to "
            + f"{self.end_stop_name}@{strtime.seconds_to_str_time(self.arrival_time)}"
        )


class TraceFootpath(MovingTrace):
    def __init__(self, start_stop_id: str, end_stop_id: str, walking_time: int):
        super().__init__(start_stop_id, end_stop_id)
        self.walking_time = walking_time

    def __str__(self):
        return f"Walk from {self.start_stop_id} to {self.end_stop_id} in {strtime.seconds_to_str_time(self.walking_time)}"


class EnrichedTraceFootpath(TraceFootpath):
    def __init__(
        self,
        trace_footpath: TraceFootpath,
        start_stop_name: str,
        end_stop_name: str,
    ):
        super().__init__(
            trace_footpath.start_stop_id,
            trace_footpath.end_stop_id,
            trace_footpath.walking_time,
        )
        self.start_stop_name = start_stop_name
        self.end_stop_name = end_stop_name

    def __str__(self):
        return f"Walk from {self.start_stop_name} ({self.start_stop_id}) to {self.end_stop_name} ({self.end_stop_id}) in {strtime.seconds_to_str_time(self.walking_time)}"
