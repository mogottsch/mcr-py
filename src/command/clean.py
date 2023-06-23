import os
from typing_extensions import Annotated
import typer

from package.clean import clean
from package.storage import write_dfs_dict


def clean_gtfs(
    gtfs_zip_file: Annotated[str, typer.Argument(help="Path to GTFS zip file")],
    output_dir: Annotated[str, typer.Argument(help="Path to output directory")],
):
    validate_flags(gtfs_zip_file)

    dfs_dict = clean(gtfs_zip_file)
    write_dfs_dict(dfs_dict, output_dir)


def validate_flags(
    gtfs_zip_file: str,
):
    # check if gtfs_zip_file exists and is a zip file
    if not os.path.exists(gtfs_zip_file):
        raise typer.BadParameter(f"File {gtfs_zip_file} does not exist.")
    if not os.path.isfile(gtfs_zip_file):
        raise typer.BadParameter(f"File {gtfs_zip_file} is not a file.")
    if not gtfs_zip_file.endswith(".zip"):
        raise typer.BadParameter(f"File {gtfs_zip_file} is not a zip file.")
