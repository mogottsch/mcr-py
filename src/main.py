from logging import INFO
from typing_extensions import Annotated
from matplotlib import logging
import typer
from command import clean, build, footpaths
from package import logger
from package.key import (
    BUILD_STRUCTURES_COMMAND_NAME,
    CLEAN_GTFS_COMMAND_NAME,
    FOOTPATHS_COMMAND_NAME,
)

# mcr-py clean-gtfs <gtfs_dir> <output_dir>
# mcr-py build-structures <clean_gtfs_dir> <output_dir>
# mcr-py calculate-footpaths \
# 		--city-id <city_id> \
# 		--osm <osm_file> \
# 		--stops <stops_file> \
# 		--avg-walking-speed <avg_walking_speed> \
# 		--max-walking-duration <max_walking_duration> \
# 		--output <output_file>
# mcr-py raptor \
# 		--city-id <city_id> \
# 		--gtfs <gtfs_dir> \
# 		--max-transfers <max_transfers> \
# 		--default-transfer-time <default_transfer_time> \
# 		--start-stop-id <start_stop_id> \
# 		--end-stop-id <end_stop_id> \
# 		--start-time <start_time:HH:MM:SS> \
# 		--output <output_file>


app = typer.Typer()
app.command(CLEAN_GTFS_COMMAND_NAME)(clean.clean_gtfs)
app.command(BUILD_STRUCTURES_COMMAND_NAME)(build.build_structures)
app.command(FOOTPATHS_COMMAND_NAME)(footpaths.generate)


@app.callback(invoke_without_command=True, no_args_is_help=True)
def main(log_level: Annotated[str, typer.Option(help="Log level.")] = "INFO"):
    logger.setup(logging.getLevelName(log_level))


if __name__ == "__main__":
    app()
