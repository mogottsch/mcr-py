import os
import pandas as pd
from multiprocessing import Pool
import multiprocessing as mp


def process_file(path, nodes):
    filename, _ = os.path.splitext(os.path.basename(path))

    labels = pd.read_feather(path)
    labels = labels[labels["osm_node_id"].isin(nodes)]
    labels["hex_id"] = filename
    return labels


def read_labels_for_nodes(dir: str, nodes: pd.Series):
    all_labels = []

    with Pool(processes=mp.cpu_count() - 2) as pool:
        files = [entry.path for entry in os.scandir(dir) if entry.is_file()]
        label_dfs = pool.starmap(process_file, [(file, nodes) for file in files])

    all_labels.extend(label_dfs)

    labels = pd.concat(all_labels)
    labels = labels.rename(
        columns={"osm_node_id": "target_id_osm", "hex_id": "start_id_hex"}
    )

    return labels