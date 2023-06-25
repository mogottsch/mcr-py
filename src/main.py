from typing_extensions import Annotated
import typer
from command import build, footpaths, gtfs, raptor
from package import logger
from package.key import (
    BUILD_STRUCTURES_COMMAND_NAME,
    FOOTPATHS_COMMAND_NAME,
    RAPTOR_COMMAND_NAME,
)

app = typer.Typer()
app.command(BUILD_STRUCTURES_COMMAND_NAME)(build.build_structures)
app.command(FOOTPATHS_COMMAND_NAME)(footpaths.generate)
app.command(RAPTOR_COMMAND_NAME)(raptor.raptor)
app.add_typer(gtfs.app, name="gtfs")


@app.callback(invoke_without_command=True, no_args_is_help=True)
def main(log_level: Annotated[str, typer.Option(help="Log level.")] = "INFO"):
    logger.setup(log_level.upper())


if __name__ == "__main__":
    app()
