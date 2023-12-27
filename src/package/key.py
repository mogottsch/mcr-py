import multiprocessing
import os
from typing import TypeVar
import tempfile


TRIPS_KEY = "trips"
STOP_TIMES_KEY = "stop_times"
STOPS_KEY = "stops"
CALENDAR_KEY = "calendar"
ROUTES_KEY = "routes"


# GTFS data keys
STOP_LAT_KEY = "stop_lat"
STOP_LON_KEY = "stop_lon"
STOP_ID_KEY = "stop_id"
ROUTE_ID_KEY = "route_id"
SERVICE_ID_KEY = "service_id"
TRIP_ID_KEY = "trip_id"
SERVICE_ID_KEY = "service_id"
STOP_NAME_KEY = "stop_name"
CALENDAR_START_DATE_KEY = "start_date"
CALENDAR_END_DATE_KEY = "end_date"
STOP_TIME_ARRIVAL_TIME_KEY = "arrival_time"
STOP_TIME_DEPARTURE_TIME_KEY = "departure_time"
STOP_SEQUENCE_KEY = "stop_sequence"
STOP_HEADSIGN_KEY = "stop_headsign"
ROUTE_SHORT_NAME_KEY = "route_short_name"
TRIP_HEADSIGN_KEY = "trip_headsign"

CALENDAR_DATE_TIME_FORMAT = "%Y%m%d"


# algorithm data
STOP_TIMES_BY_TRIP_KEY = "stop_times_by_trip"
TRIP_IDS_BY_ROUTE_KEY = "trip_ids_by_route"
STOPS_BY_ROUTE_KEY = "stops_by_route"
ROUTES_BY_STOP_KEY = "routes_by_stop"
IDX_BY_STOP_BY_ROUTE_KEY = "idx_by_stop_by_route"
TIMES_BY_STOP_BY_TRIP_KEY = "times_by_stop_by_trip"
STOP_ID_SET_KEY = "stop_id_set"
ROUTE_ID_SET_KEY = "route_id_set"
TRIP_ID_SET_KEY = "trip_id_set"
FOOTPATHS_KEY = "footpaths"


# command names
BUILD_STRUCTURES_COMMAND_NAME = "build-structures"
FOOTPATHS_COMMAND_NAME = "generate-footpaths"
RAPTOR_COMMAND_NAME = "raptor"
MCR_COMMAND_NAME = "mcr"

GTFS_UPPER_COMMAND_NAME = "gtfs"
GTFS_LIST_COMMAND_NAME = "list"
GTFS_DOWNLOAD_COMMAND_NAME = "download"
GTFS_CROP_COMMAND_NAME = "crop"
GTFS_CLEAN_COMMAND_NAME = "clean"
GTFS_SUB_UPPER_READ_COMMAND_NAME = "read"
GTFS_READ_STOPS_COMMAND_NAME = "stops"

OSM_UPPER_COMMAND_NAME = "osm"
OSM_LIST_COMMAND_NAME = "list"

COMPLETE_GTFS_CLEAN_COMMAND_NAME = (
    f"{GTFS_UPPER_COMMAND_NAME} {GTFS_CLEAN_COMMAND_NAME}"
)


# paths
TMP_DIR_LOCATION = os.path.expanduser(
    os.environ.get("MCR_PY_TMP_DIR", tempfile.gettempdir())
)
ROOT_TMP_DIR_NAME = "mcr-py"
TMP_OSM_DIR_NAME = "osm"
TMP_GTFS_DIR_NAME = "gtfs"
TMP_GTFS_CATALOG_FILE_NAME = "catalog.csv"

## output
RAPTOR_ARRIVAL_TIMES_FILE_NAME = "arrival_times.csv"
RAPTOR_TRACE_FILE_NAME = "tracer_map.pkl"


# urls
GTFS_CATALOG_URL = "https://bit.ly/catalogs-csv"  # https://database.mobilitydata.org/


DATE_TIME_FORMAT = "%d.%m.%Y-%H:%M:%S"
DATE_TIME_FORMAT_HUMAN_READABLE = "DD.MM.YYYY-HH:MM:SS"

# pkl data keys
TRACER_MAP_KEY = "tracer_map"

S = TypeVar("S")  # additional information about stop
T = TypeVar("T")  # additional information about trip

DEFAULT_N_PROCESSES = multiprocessing.cpu_count() - 2
