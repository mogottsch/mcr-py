from typing_extensions import Annotated
import typer

from package import key, gtfs


app = typer.Typer()


@app.command(name=key.GTFS_LIST_COMMAND_NAME)
def list_command(
    country_code: Annotated[str, typer.Option(help="Country code used to filter")] = "",
    subdivision: Annotated[str, typer.Option(help="Subdivision used to filter")] = "",
    municipality: Annotated[str, typer.Option(help="Municipality used to filter")] = "",
):
    gtfs.list_catalog(country_code, subdivision, municipality)


@app.command(name=key.GTFS_DOWNLOAD_COMMAND_NAME)
def download_command():
    pass


@app.callback(invoke_without_command=True, no_args_is_help=True)
def main():
    pass
