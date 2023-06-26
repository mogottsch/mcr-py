from package import key


GTFS_DTYPES = {
    key.TRIP_ID_KEY: "string",
    key.STOP_ID_KEY: "string",
    key.ROUTE_ID_KEY: "string",
    key.SERVICE_ID_KEY: "string",
    key.STOP_TIME_ARRIVAL_TIME_KEY: "string",
    key.STOP_TIME_DEPARTURE_TIME_KEY: "string",
    key.STOP_SEQUENCE_KEY: "int64",
    key.STOP_HEADSIGN_KEY: "string",
    key.STOP_LAT_KEY: "float64",
    key.STOP_LON_KEY: "float64",
}
