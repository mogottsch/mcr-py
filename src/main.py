from typing_extensions import Annotated
import typer
from command import build, footpaths, raptor, osm
from command.gtfs import gtfs
from package import logger, key

app = typer.Typer(pretty_exceptions_show_locals=False)
app.command(key.BUILD_STRUCTURES_COMMAND_NAME)(build.build_structures)
app.command(key.FOOTPATHS_COMMAND_NAME)(footpaths.generate)
app.command(key.RAPTOR_COMMAND_NAME)(raptor.raptor)
app.add_typer(gtfs.app, name=key.GTFS_UPPER_COMMAND_NAME)
app.add_typer(osm.app, name=key.OSM_UPPER_COMMAND_NAME)


@app.callback(invoke_without_command=True, no_args_is_help=True)
def main(log_level: Annotated[str, typer.Option(help="Log level.")] = "INFO"):
    logger.setup(log_level.upper())


if __name__ == "__main__":
    app()
