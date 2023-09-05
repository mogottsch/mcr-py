import os
import pandas as pd


def read_labels_for_nodes(dir: str, nodes: pd.Series):
    all_labels = []
    for path in os.scandir(dir):
        filename, _ = os.path.splitext(os.path.basename(path))

        labels = pd.read_feather(path)
        labels = labels[labels["osm_node_id"].isin(nodes)]
        labels["hex_id"] = filename
        all_labels.append(labels)

    labels = pd.concat(all_labels)
    labels = labels.rename(
        columns={"osm_node_id": "target_id_osm", "hex_id": "start_id_hex"}
    )

    return labels
