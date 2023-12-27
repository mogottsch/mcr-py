from package import strtime
import pandas as pd


PROFILE_MAX_TIME = strtime.str_time_to_seconds("48:00:00")


def profile_calculation_worker(types: list[str], args: tuple[str, pd.DataFrame]):
    name, group = args
    profile = calculate_profile_for_group(group, types)
    if profile is not None:
        return name, profile
    return None


def calculate_profile_for_group(group, types):
    costs = group["cost"].unique()
    costs.sort()

    profile = []

    for cost in costs:
        labels_for_cost = group[group["cost"] <= cost]
        curr_time = next_larger_minute(labels_for_cost["time"].min())
        while True:
            labels_for_cost_and_time = labels_for_cost[
                labels_for_cost["time"] <= curr_time
            ]

            all_reached = labels_for_cost_and_time[types].sum().min() != 0
            curr_time += 60

            if all_reached:
                profile.append((cost, curr_time))
                break

            if curr_time > PROFILE_MAX_TIME:
                start_id_hex = group["start_id_hex"].iloc[0]
                raise ValueError(
                    f"Time limit exceeded for hex id {start_id_hex} at cost {cost}"
                )

    return profile


def next_larger_minute(seconds_since_midnight: int) -> int:
    return (seconds_since_midnight // 60 + 1) * 60


def build_profiles_df(profiles, start_time: int):
    profiles = [
        (hex_id, cost, time)
        for hex_id, profile in profiles.items()
        for cost, time in profile
    ]
    profiles_df = pd.DataFrame(profiles, columns=["hex_id", "cost", "time"])
    profiles_df["time"] = profiles_df["time"] - start_time
    profiles_df = profiles_df.pivot(index="hex_id", columns="cost", values="time")

    profiles_df.columns = [f"cost_{c}" for c in profiles_df.columns]

    profiles_df = fill_columns_by_left(profiles_df)

    return profiles_df


def fill_columns_by_left(profiles_df: pd.DataFrame) -> pd.DataFrame:
    # fill first cost in case it is not possible to reach without any cost (e.g. car, that can't stop for some time)
    profiles_df["cost_0"] = profiles_df["cost_0"].fillna(float("inf"))
    for c in profiles_df.columns:
        if c == "cost_0":
            continue
        previous_column = profiles_df.columns[profiles_df.columns.get_loc(c) - 1]
        profiles_df[c] = profiles_df[c].fillna(profiles_df[previous_column])
    profiles_df = profiles_df.astype(float)
    return profiles_df


def add_any_column_is_different_column(profiles_df: pd.DataFrame) -> pd.DataFrame:
    profiles_df["any_column_different"] = False
    for c in profiles_df.columns:
        if not c.startswith("cost_") or c == "cost_0":
            continue
        previous_column = profiles_df.columns[profiles_df.columns.get_loc(c) - 1]
        profiles_df["any_column_different"] = profiles_df["any_column_different"] | (
            profiles_df[c] != profiles_df[previous_column]
        )
    return profiles_df


def add_required_cost_for_optimum_column(profiles_df: pd.DataFrame) -> pd.DataFrame:
    cost_rows = [c for c in profiles_df.columns if c.startswith("cost_")]

    def calculate_required_cost_for_optimal_for_row(row):
        optimal = min(row[cost_rows])
        for c in cost_rows:
            if row[c] == optimal:
                return int(c[len("cost_") :])
        raise ValueError("No optimal cost found")

    profiles_df["required_cost_for_optimal"] = profiles_df.apply(
        calculate_required_cost_for_optimal_for_row, axis=1
    )
    return profiles_df


def add_optimum_column(profiles_df: pd.DataFrame) -> pd.DataFrame:
    cost_rows = [c for c in profiles_df.columns if c.startswith("cost_")]

    def calculate_optimal_for_row(row):
        optimal = min(row[cost_rows])
        for c in cost_rows:
            if row[c] == optimal:
                return row[c]
        raise ValueError("No optimal cost found")

    profiles_df["optimal"] = profiles_df.apply(calculate_optimal_for_row, axis=1)
    return profiles_df
