from typing import Annotated
import typer

def build_structures(
	clean_gtfs_dir: Annotated[str, typer.Argument(help="Path to clean GTFS directory")],
	output_dir: Annotated[str, typer.Argument(help="Path to output directory")],
	):
	trips
