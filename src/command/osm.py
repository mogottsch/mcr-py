from typing_extensions import Annotated
import typer

from package import key
from package.osm import osm


app = typer.Typer()


@app.command(
    name=key.OSM_LIST_COMMAND_NAME,
    help="List all available OSM data",
)
def list_command(
    selector: Annotated[
        str, typer.Option(help="Selector in dot notation, e.g. '.regions.africa'")
    ] = ""
):
    osm.list_available(selector)


@app.callback(invoke_without_command=True, no_args_is_help=True)
def main():
    pass
