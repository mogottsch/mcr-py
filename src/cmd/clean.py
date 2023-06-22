import os
from typing_extensions import Annotated
import typer

from pkg.clean import clean

def clean_gtfs(
	gtfs_zip_file: Annotated[str, typer.Argument(help="Path to GTFS zip file")],
	output_dir: Annotated[str, typer.Argument(help="Path to output directory")],
	):
	# check if gtfs_zip_file exists and is a zip file
	if not os.path.exists(gtfs_zip_file):
		raise typer.BadParameter(f"File {gtfs_zip_file} does not exist.")
	if not os.path.isfile(gtfs_zip_file):
		raise typer.BadParameter(f"File {gtfs_zip_file} is not a file.")
	endswith = gtfs_zip_file.endswith(".zip")

	clean(gtfs_zip_file, output_dir)
