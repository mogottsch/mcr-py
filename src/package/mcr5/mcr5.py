import multiprocessing
from multiprocessing import Process, Queue
import psutil
import time
import os

from package.mcr.data import MCRGeoData
from package.mcr.mcr import MCR
from package.mcr.output import OutputFormat
from package.mcr5.h3_osm_interaction import H3OSMLocationMapping

n_cores = multiprocessing.cpu_count()
DEFAULT_MAX_PROCESSES = n_cores - 1


class MCR5:
    def __init__(
        self,
        geo_data: MCRGeoData,
        min_free_memory: float = 3.0,
        max_processes: int = DEFAULT_MAX_PROCESSES,
    ):
        self.geo_data = geo_data
        self.min_free_memory = min_free_memory
        self.max_processes = max_processes

    def run(
        self,
        location_mappings: list[H3OSMLocationMapping],
        start_time: str,
        output_dir: str,
        max_transfers: int = 2,
    ) -> list[tuple[str, Exception]]:
        """
        Run a MCR5 analysis on the underlying geo data for each location mapping.
        Returns a list of tuples, which represent errors that occurred during the analysis.
        The first element of the tuple is the h3 cell, the second element is the exception.
        """
        processes = []
        errors = Queue()

        for location_mapping in location_mappings:
            h3_cell = location_mapping.h3_cell
            osm_node_id = location_mapping.osm_node_id

            while (
                self.get_active_process_count(processes) >= self.max_processes
                or get_available_memory() < self.min_free_memory
            ):
                self.print_status(processes, location_mappings)
                time.sleep(1)

            p = Process(
                target=self.run_mcr,
                args=(
                    errors,
                    h3_cell,
                    osm_node_id,
                    self.geo_data,
                    start_time,
                    max_transfers,
                    output_dir,
                ),
            )

            p.start()
            processes.append(p)

        while self.get_active_process_count(processes) > 0:
            self.print_status(processes, location_mappings)
            time.sleep(1)

        for p in processes:
            p.join()

        print("All processes finished.                                  ")

        errors = [errors.get() for _ in range(errors.qsize())]
        return errors

    def run_mcr(
        self,
        errors: Queue,
        h3_cell: str,
        osm_node_id: int,
        mcr_geo_data: MCRGeoData,
        start_time: str,
        max_transfers: int,
        output_dir: str,
    ) -> None:
        mcr_runner = MCR(
            mcr_geo_data, disable_paths=True, output_format=OutputFormat.DF_FEATHER
        )
        output = os.path.join(output_dir, f"{h3_cell}.feather")

        try:
            mcr_runner.run(
                start_node_id=osm_node_id,
                start_time=start_time,
                max_transfers=max_transfers,
                output_path=output,
            )
        except Exception as e:
            errors.put(
                (
                    {
                        "h3_cell": h3_cell,
                        "osm_node_id": osm_node_id,
                        "start_time": start_time,
                        "max_transfers": max_transfers,
                        "output_path": output,
                    },
                    e,
                )
            )

    def print_status(
        self, processes: list[Process], location_mappings: list[H3OSMLocationMapping]
    ):
        available_memory = pretty_bytes(get_available_memory())
        active_processes = self.get_active_process_count(processes)
        started_processes = len(processes)
        total_processes = len(location_mappings)

        print(
            f"Available memory: {available_memory} | active: {active_processes} | started: {started_processes}/{total_processes}        ",
            end="\r",
        )

    def get_active_process_count(self, processes: list[Process]) -> int:
        return sum(p.is_alive() for p in processes)


def get_available_memory() -> int:
    return psutil.virtual_memory().available


def pretty_bytes(b: float) -> str:
    for unit in ["B", "KiB", "MiB", "GiB", "TiB", "PiB"]:
        if b < 1024:
            return f"{b:.2f}{unit}"
        b /= 1024
    return f"{b:.2f}EiB"


# run()
