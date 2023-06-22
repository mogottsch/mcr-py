import typer

app = typer.Typer()

@app.callback()
def default_command():
    """
    This is the root command.
    """
    typer.echo("Hello, I am the root command!")

@app.command()
def clean_gtfs():
    """
    Command for cleaning GTFS data.
    """
    typer.echo("Cleaning GTFS data...")

@app.command()
def build_structures():
    """
    Command for building structures.
    """
    typer.echo("Building structures...")

if __name__ == "__main__":
    app()
