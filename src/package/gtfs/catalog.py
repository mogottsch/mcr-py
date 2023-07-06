from re import IGNORECASE
from typing_extensions import Any

from geopandas import pd
import os
from osmnx.elevation import requests
from rich.console import Console
from rich.table import Table
from rich import print

from package import key, storage
from package.logger import Timed
from package.logger import llog

CATALOG_PATH = storage.get_tmp_path(
    key.TMP_GTFS_DIR_NAME, key.TMP_GTFS_CATALOG_FILE_NAME
)

COL_DATA_TYPE = "data_type"
COL_COUNTRY_CODE = "location.country_code"
COL_SUBDIVISION_NAME = "location.subdivision_name"
COL_MUNICIPALITY = "location.municipality"
COL_PROVIDER = "provider"
COL_NAME = "name"
COL_DOWNLOAD_URL = "urls.direct_download"
COL_AUTH_TYPE = "urls.authentication_type"

RELEVANT_COLUMNS = [
    COL_DATA_TYPE,
    COL_COUNTRY_CODE,
    COL_SUBDIVISION_NAME,
    COL_MUNICIPALITY,
    COL_PROVIDER,
    COL_NAME,
    COL_DOWNLOAD_URL,
    COL_AUTH_TYPE,
]


def list_catalog(country_code: str, subdivision_name: str, municipality: str):
    """List all available GTFS feeds."""
    catalog = get_catalog()
    catalog = filter_catalog(catalog, country_code, subdivision_name, municipality)

    print_catalog(catalog)


def get_catalog() -> pd.DataFrame:
    if not os.path.exists(CATALOG_PATH):
        download_catalog()

    catalog = pd.read_csv(CATALOG_PATH)
    catalog = catalog[RELEVANT_COLUMNS]
    catalog = pre_filter_catalog(catalog)
    catalog = catalog.fillna("")
    return catalog


def download_catalog():
    request = requests.get(key.GTFS_CATALOG_URL)
    os.makedirs(os.path.dirname(CATALOG_PATH), exist_ok=True)
    with open(CATALOG_PATH, "wb") as f:
        f.write(request.content)


def pre_filter_catalog(catalog: pd.DataFrame) -> pd.DataFrame:
    catalog = catalog[catalog[COL_DATA_TYPE] == "gtfs"]
    catalog = catalog[(catalog[COL_AUTH_TYPE] != 1) & (catalog[COL_AUTH_TYPE] != 2)]
    return catalog


def filter_catalog(
    catalog: pd.DataFrame, country_code: str, subdivision_name: str, municipality: str
) -> pd.DataFrame:
    if country_code:
        catalog = catalog[
            catalog[COL_COUNTRY_CODE].str.contains(country_code, flags=IGNORECASE)
        ]
    if subdivision_name:
        catalog = catalog[
            catalog[COL_SUBDIVISION_NAME].str.contains(
                subdivision_name, flags=IGNORECASE
            )
        ]
    if municipality:
        catalog = catalog[
            catalog[COL_MUNICIPALITY].str.contains(municipality, flags=IGNORECASE)
        ]
    return catalog


def print_catalog(catalog: pd.DataFrame):
    if len(catalog) == 0:
        print("[i] No GTFS feeds found.[/i]")
        return

    table = Table(title="GTFS Catalog", show_lines=True)

    index_max_length = max(int(catalog.index.astype(str).str.len().max()), len("ID"))
    country_code_max_length = max(
        int(catalog[COL_COUNTRY_CODE].str.len().max()), len("Code")
    )
    subdivision_max_length = max(
        int(catalog[COL_SUBDIVISION_NAME].str.len().max()), len("Subdivision")
    )
    municipality_max_length = max(
        int(catalog[COL_MUNICIPALITY].str.len().max()), len("Municipality")
    )

    ID_COLOR = "magenta"
    table.add_column("ID", style=ID_COLOR, width=index_max_length)
    table.add_column("Code", style="cyan", width=country_code_max_length)
    table.add_column("Subdivision", width=subdivision_max_length)
    table.add_column("Municipality", width=municipality_max_length)
    table.add_column("Provider")

    for index, row in catalog.iterrows():
        table.add_row(
            format_value(index),
            format_value(row[COL_COUNTRY_CODE]),  # type: ignore
            format_value(row[COL_SUBDIVISION_NAME]),  # type: ignore
            format_value(row[COL_MUNICIPALITY]),  # type: ignore
            format_value(row[COL_PROVIDER]),  # type: ignore
        )

    console = Console()
    console.print(table)
    print(f"Total: [bold]{len(catalog)}[/bold]\n")
    print(
        f"[i] Use [bold]{key.GTFS_UPPER_COMMAND_NAME} {key.GTFS_DOWNLOAD_COMMAND_NAME} <[{ID_COLOR}]ID[/]>[/bold] to download a GTFS feed."
    )


def format_value(value: Any) -> str:
    formatted_value = str(value) or "-"
    return formatted_value


def download(id: int, output: str):
    catalog = get_catalog()
    catalog = catalog[catalog.index == id]

    if len(catalog) == 0:
        llog.error(f"GTFS feed with ID {id} not found.")
        return

    row = catalog.iloc[0]
    url = row[COL_DOWNLOAD_URL]

    if not url:
        llog.error(f"GTFS feed with ID {id} has no download URL.")
        return

    with Timed.info(f"Downloading GTFS feed with ID {id}"):
        storage.download_file(url, output)
