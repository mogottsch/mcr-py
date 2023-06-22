import pandas as pd
import zipfile
import os

STOPS_FILE = "stops.txt"
ROUTES_FILE = "routes.txt"
TRIPS_FILE = "trips.txt"
STOP_TIMES_FILE = "stop_times.txt"
TRANSFERS_FILE = "transfers.txt"

EXPECTED_FILES = [
	# STOPS_FILE,
	# ROUTES_FILE,
	TRIPS_FILE,
	STOP_TIMES_FILE,
	# TRANSFERS_FILE
]


def clean(gtfs_zip_path: str, output_path: str):
	"""
	Cleans the GTFS data and writes the cleaned data to the output path.
	The resulting files are `trips.csv` and `stop_times.csv`, other files are
	not needed for our algorithms.
	"""
	dfs = read_dfs(gtfs_zip_path)
	trips_df, stop_times_df = dfs["trips"], dfs["stop_times"]

	trips_df = split_routes(trips_df, stop_times_df)
	trips_df = add_first_stop_info(trips_df, stop_times_df)


	# make dir with parents
	write_dfs(trips_df, stop_times_df, output_path)
	
	

def read_dfs(gtfs_zip_path: str) -> dict[str, pd.DataFrame]:
	"""
	Reads GTFS zip file and returns a dictionary of dataframes.
	"""
	dfs = {}

	with zipfile.ZipFile(gtfs_zip_path, 'r') as zip_ref:
		contained = zip_ref.namelist()

		for expected_file in EXPECTED_FILES:
			if expected_file not in contained:
				raise Exception(f'Expected file {expected_file} not in zip file')

		for file in EXPECTED_FILES:
			df = read_file(zip_ref, file)
			name = file.split('.')[0]
			dfs[name] = df

	return dfs

def read_file(zip_ref: zipfile.ZipFile, file: str) -> pd.DataFrame:
	with zip_ref.open(file) as f:
		df = pd.read_csv(f)
		return df


def split_routes(trips_df: pd.DataFrame, stop_times_df: pd.DataFrame) -> pd.DataFrame:
	"""
	Splits routes into one route per actual path.

	In GTFS data one route can have multiple paths, e.g. one train route mostly
	has two directions. However, sometimes even routes with the same direction 
	can have different paths.
	For our algorithms it is easier to have one route per path.
	"""
	# first we backup the old route_ids for debugging purposes
	trips_df["old_route_id"] = trips_df["route_id"]

	split_routes_by_direction(trips_df)
	paths_df = create_paths_df(trips_df, stop_times_df)
	paths_df = add_unique_route_ids(paths_df)
	trips_df = update_route_ids(trips_df, paths_df)

	return trips_df


def split_routes_by_direction(trips_df: pd.DataFrame):
	trips_df["route_id"] = trips_df["route_id"] + "_" + trips_df["direction_id"].astype(str)

def create_paths_df(trips_df: pd.DataFrame, stop_times_df: pd.DataFrame) -> pd.DataFrame:
	"""
	Creates a dataframe route_id, trip_id, and path, where path is a string 
	representation of the stops on the route in order.
	"""
	trips_stop_times_df = pd.merge(trips_df, stop_times_df, on="trip_id")
	paths_df = (
		trips_stop_times_df.sort_values(["route_id", "trip_id", "stop_sequence"])
		.groupby(["route_id", "trip_id"])["stop_id"]
		.apply(list)
		.apply(str)
		.reset_index()
	)
	paths_df = paths_df.rename(columns={"stop_id": "path"})
	return paths_df


def add_unique_route_ids(paths_df: pd.DataFrame) -> pd.DataFrame:
	"""
	Adds a new column `new_route_id` to the dataframe, which is a unique route_id 
	for each path.
	"""
	known_route_paths = {}
	path_counter_per_route = {}
	path_id_by_path = {}

	paths_df["new_route_id"] = paths_df["route_id"]

	for i,row in paths_df.iterrows():
		path_counter = path_counter_per_route.get(row["route_id"], 0)
		known_paths = known_route_paths.get(row["route_id"], set())

		path_id = None

		path = row["path"]
		if path in known_paths:
			path_id = path_id_by_path[path]
		else:
			path_id = chr(ord('A') + path_counter)
			paths_df.at[i, "new_route_id"] = row["route_id"] + "_" + path_id

			known_paths.add(path)
			known_route_paths[row["route_id"]] = known_paths

			path_counter_per_route[row["route_id"]] = path_counter + 1

			path_id_by_path[path] = path_id

		paths_df.at[i, "new_route_id"] = row["route_id"] + "_" + path_id

	return paths_df.drop(columns=["path"]) # we don't need the path column anymore

def update_route_ids(trips_df: pd.DataFrame, paths_df: pd.DataFrame) -> pd.DataFrame:
	trips_df = trips_df.merge(paths_df, on=["route_id", "trip_id"])
	trips_df["route_id"] = trips_df["new_route_id"]
	trips_df = trips_df.drop(columns=["new_route_id"])
	return trips_df

def add_first_stop_info(trips_df: pd.DataFrame, stop_times_df: pd.DataFrame) -> pd.DataFrame:
# add first stop id to trips
	first_stop_times = (
		stop_times_df.sort_values(["trip_id", "stop_sequence"])
		.groupby("trip_id")
		.first()[["stop_id", "departure_time"]]
		.rename(
			columns={"stop_id": "first_stop_id", "departure_time": "trip_departure_time"}
		)
	)

	return trips_df.merge(
		first_stop_times, left_on="trip_id", right_index=True, how="left"
	)

def write_dfs(trips_df: pd.DataFrame, stop_times_df: pd.DataFrame, output_path: str):
	os.makedirs(output_path, exist_ok=True)
	for file in os.listdir(output_path):
		os.remove(os.path.join(output_path, file))

	trips_df.to_csv(os.path.join(output_path, "trips.csv"), index=False)
	stop_times_df.to_csv(os.path.join(output_path, "stop_times.csv"), index=False)
