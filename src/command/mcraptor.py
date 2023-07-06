from typing import Optional
from typing_extensions import Annotated
from geopandas import pd

from pyrosm.data import os
import typer
from rich.pretty import pprint

from package import storage
from package import key
from package.mcraptor import McRaptor
from package.structs import build
from package.key import BUILD_STRUCTURES_COMMAND_NAME, FOOTPATHS_COMMAND_NAME
from package.logger import Timed

FOOTPATHS_HELP = f"""
A path that should point to a pickle file containing footpaths, as generated \
by the {FOOTPATHS_COMMAND_NAME} command.
"""

STRUCTS_HELP = f"""
A path that should point to a pickle file containing structures, as generated \
by the {BUILD_STRUCTURES_COMMAND_NAME} command.
"""


def mcraptor(
    footpaths: Annotated[str, typer.Option(help=FOOTPATHS_HELP)],
    structs: Annotated[str, typer.Option(help=STRUCTS_HELP)],
    start_stop_id: Annotated[str, typer.Option(help="Start stop ID")],
    start_time: Annotated[str, typer.Option(help="Start time in HH:MM:SS")],
    output_dir: Annotated[str, typer.Option(help="Output directory")],
    end_stop_id: Annotated[Optional[str], typer.Option(help="End stop ID")] = None,
    max_transfers: Annotated[int, typer.Option(help="Maximum number of transfers")] = 3,
    default_transfer_time: Annotated[
        int, typer.Option(help="Transfer time used when tranfering at the same stop")
    ] = 180,
):
    validate_flags(
        footpaths,
        structs,
        max_transfers,
        default_transfer_time,
        start_stop_id,
        end_stop_id,
        start_time,
        output_dir,
    )

    footpaths_dict = storage.read_any_dict(footpaths)
    if not "footpaths" in footpaths_dict:
        raise typer.BadParameter(f"Footpaths file {footpaths} has unexpected format.")
    footpaths_dict = footpaths_dict["footpaths"]

    structs_dict = storage.read_any_dict(structs)
    build.validate_structs_dict(structs_dict)

    with Timed.info("Running RAPTOR"):
        r = McRaptor(
            structs_dict,
            footpaths_dict,
            max_transfers,
            default_transfer_time,
        )
        labels = r.run(
            start_stop_id,
            end_stop_id,
            start_time,
        )

    storage.write_any_dict(
        {key.MC_RAPTOR_LABELS_KEY: labels},
        os.path.join(output_dir, key.MC_RAPTOR_LABELS_FILE_NAME),
    )


def validate_flags(
    footpaths: str,
    structs: str,
    max_transfers: int,
    default_transfer_time: int,
    start_stop_id: str,
    end_stop_id: Optional[str],
    start_time: str,
    output: str,
):
    if not os.path.exists(footpaths):
        raise typer.BadParameter(
            f"Footpaths file {os.path.abspath(footpaths)} does not exist."
        )

    if not os.path.exists(structs):
        raise typer.BadParameter(f"Structs file {structs} does not exist.")

    if max_transfers < 0:
        raise typer.BadParameter(f"Max transfers must be non-negative.")

    if default_transfer_time < 0:
        raise typer.BadParameter(f"Default transfer time must be non-negative.")
