Q = {}
for k in range(MAX_TRANSFERS):
    Q = {}

    for stop_id in marked_stops:
        for route_id in get_routes_serving_stop(stop_id):
            if route_id not in Q:
                Q[route_id] = stop_id
                continue

            # if our stop is closer to the start than the existing one, we replace it
            existing_stop_id = Q[route_id]
            idx = get_idx_of_stop_in_route(stop_id, route_id)
            existing_idx = get_idx_of_stop_in_route(existing_stop_id, route_id)
            if idx < existing_idx:
                Q[route_id] = stop_id

        marked_stops.remove(stop_id)

    for route_id, stop_id in Q.items():
        trip_id = None
        for stop_id, _ in stops_by_route[route_id]:
            arrival_time = get_arrival_time(trip_id, stop_id)
            if trip_id is not None and arrival_time < min(
                tau_best[stop_id], tau_best[end_stop_id]
            ):
                tau_i[k][stop_id] = get_arrival_time(trip_id, stop_id)
