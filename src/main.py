from typing_extensions import Annotated
from matplotlib import logging
import typer
from command import clean, build, footpaths, raptor
from package import logger
from package.key import (
    BUILD_STRUCTURES_COMMAND_NAME,
    CLEAN_GTFS_COMMAND_NAME,
    FOOTPATHS_COMMAND_NAME,
    RAPTOR_COMMAND_NAME,
)

app = typer.Typer()
app.command(CLEAN_GTFS_COMMAND_NAME)(clean.clean_gtfs)
app.command(BUILD_STRUCTURES_COMMAND_NAME)(build.build_structures)
app.command(FOOTPATHS_COMMAND_NAME)(footpaths.generate)
app.command(RAPTOR_COMMAND_NAME)(raptor.raptor)


@app.callback(invoke_without_command=True, no_args_is_help=True)
def main(log_level: Annotated[str, typer.Option(help="Log level.")] = "INFO"):
    logger.setup(logging.getLevelName(log_level))


if __name__ == "__main__":
    app()
