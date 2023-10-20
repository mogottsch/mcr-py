import logging
from multiprocessing import Process, Queue
import pickle
import psutil
import time
import os
from package import key
from tqdm.auto import tqdm

from package.mcr.data import OSMData
from package.mcr.mcr import MCR
from package.mcr.output import OutputFormat
from package.mcr5.h3_osm_interaction import H3OSMLocationMapping
from package.logger import (
    copy_settings_to_root_logger,
    make_string_stream_logger,
    rlog,
)


class MCR5:
    def __init__(
        self,
        geo_data: OSMData,
        min_free_memory: float = 3.0,
        max_processes: int = key.DEFAULT_N_PROCESSES,
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
        errors = Queue(maxsize=-1)

        p_id_hex_id_map = {}

        # ensure ouptput directory exists
        os.makedirs(output_dir, exist_ok=True)

        errors_list = []
        pbar = tqdm(location_mappings, desc="Starting")
        for location_mapping in location_mappings:
            h3_cell = location_mapping.h3_cell
            osm_node_id = location_mapping.osm_node_id

            while (
                self.get_active_process_count(processes) >= self.max_processes
                or get_available_memory() < self.min_free_memory
            ):
                errors_list.extend([errors.get() for _ in range(errors.qsize())])
                if errors.full():
                    raise Exception("Error queue is full.")
                self.print_status(processes, pbar)
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
            p_id_hex_id_map[p.pid] = h3_cell

        while self.get_active_process_count(processes) > 0:
            self.print_status(processes, pbar)
            errors_list.extend([errors.get() for _ in range(errors.qsize())])
            if errors.full():
                raise Exception("Error queue is full.")
            time.sleep(1)
        pbar.update(len(location_mappings) - pbar.n)
        pbar.close()

        for p in processes:
            p.join()

        rlog.info("All processes finished.")

        while not errors.empty():
            errors_list.append(errors.get())
        errors = errors_list
        if len(errors) > 0:
            rlog.warning(f"{len(errors)} errors occurred during the analysis.")

        with open(os.path.join(output_dir, "errors.pkl"), "wb") as f:
            pickle.dump(errors, f)

        return errors

    def run_mcr(
        self,
        errors: Queue,
        h3_cell: str,
        osm_node_id: int,
        mcr_geo_data: OSMData,
        start_time: str,
        max_transfers: int,
        output_dir: str,
    ) -> None:
        output = os.path.join(output_dir, f"{h3_cell}.feather")

        l, log_stream = make_string_stream_logger(f"mcr5-{h3_cell}", logging.DEBUG)
        copy_settings_to_root_logger(l)
        try:
            mcr_runner = MCR(
                mcr_geo_data,
                disable_paths=True,
                enable_limit=True,
                output_format=OutputFormat.DF_FEATHER,
                logger=l,
            )

            mcr_runner.run(
                start_node_id=osm_node_id,
                start_time=start_time,
                max_transfers=max_transfers,
                output_path=output,
            )
        except BaseException as e:
            log_stream_value = log_stream.getvalue()
            errors.put(
                {
                    "h3_cell": h3_cell,
                    "osm_node_id": osm_node_id,
                    "start_time": start_time,
                    "max_transfers": max_transfers,
                    "output_path": output,
                    "error": e.__repr__(),  # the exception object might not be pickable
                    "logs": log_stream_value,
                },
            )

    def print_status(
        self,
        processes: list[Process],
        pbar: tqdm,
    ):
        available_memory = pretty_bytes(get_available_memory())
        active_processes_count = self.get_active_process_count(processes)

        started_processes = len(processes)
        finished_processes = started_processes - active_processes_count

        pbar.update(finished_processes - pbar.n)
        pbar.set_description(
            f"Available memory: {available_memory} | active: {active_processes_count}         ",
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
