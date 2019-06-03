# coding: utf-8
"""
A module for extracting datapoints from CSV files.
"""
import csv
import logging
import sys
import threading
import time
from collections import defaultdict
from itertools import chain
from operator import itemgetter
from typing import Dict

from cognite.client import APIError
from cognite.client.stable.datapoints import Datapoint, TimeseriesWithDatapoints
from cognite.client.stable.time_series import TimeSeries

logger = logging.getLogger(__name__)


BATCH_MAX = 1000  # Maximum number of time series batched at once


def extract_data_points(client, monitor, time_series_cache, live_mode: bool, folder_path, failed_path, finished_path):
    """Find and publish all data points in files found in 'folder_path'.

    In `live_mode` will process only 20 newest files, and the search for new files again.
    If not live mode, it will start with oldest files first, and process all then quit.
    """
    while True:
        files = find_files_in_path(folder_path)

        logger.info("Found {} relevant files to process in {}".format(len(files), folder_path))
        monitor.available_csv_files_gauge.set(len(files))
        monitor.push()

        if files:
            files = files[:20] if live_mode else files  # We only process 20 newest before we look again for live
            process_files(client, monitor, files, time_series_cache, failed_path, finished_path)

        if live_mode:
            time.sleep(8)
        else:
            logger.info("Extraction complete")
            break


def get_all_time_series(client):
    """Return map of time series externalId -> name of all time series that has externalId."""
    for i in range(10):
        try:
            res = client.time_series.get_time_series(include_metadata=True, autopaging=True)
        except APIError as exc:
            logger.error("Failed to get timeseries: {!s}".format(exc))
            time.sleep(i)
        else:
            break
    else:
        logger.fatal("Could not fetch time series data from CDP, exiting!")
        sys.exit(1)

    return {
        i["metadata"]["externalID"]: i["name"]
        for i in res.to_json()
        if "metadata" in i and "externalID" in i["metadata"]
    }


def _log_error(func, *args, **vargs):
    """Call 'func' with args, then log if an exception was raised."""
    try:
        return func(*args, **vargs)
    except Exception as error:
        logger.info(error)


def create_data_points(values, timestamps):
    """Return CDP Datapoint object for 'values' and 'timestamps'."""
    data_points = []

    for i, value_string in enumerate(values):
        if value_string:
            try:
                value = float(value_string.replace(",", "."))
            except ValueError as error:
                logger.info(error)
                continue
            data_points.append(Datapoint(timestamp=int(timestamps[i]) * 1000, value=value))

    return data_points


def create_time_series(client, name: str, external_id: str) -> None:
    """Create a new time series when the 'external_id' isn't found."""
    new_time_series = TimeSeries(
        name=name, description="Auto-generated time series, external ID not found", metadata={"externalID": external_id}
    )
    _log_error(client.time_series.post_time_series, [new_time_series])


def get_parsed_file(path) -> Dict[str, list]:
    """Parse the csv file and return the data in a {col_name -> list_of_row_items} dictionary"""
    parsed_file = defaultdict(list)
    with open(path, "r", encoding="latin-1") as f:
        data = csv.DictReader(f, delimiter=";")
        for row in data:
            for k, v in row.items():
                parsed_file[k].append(v)
    return parsed_file


def process_csv_file(client, monitor, csv_path, existing_time_series):
    start_time = time.time()

    parsed_file = get_parsed_file(csv_path)
    timestamps = parsed_file[""][1:]  # ignore garbage value in first line
    del parsed_file[""]  # remove the timestamps from the dictionary

    count_of_data_points = 0
    unique_external_ids = set()  # Count number of time series processed
    current_time_series = []  # List of time series being processed
    network_threads = []
    for col_name, v in parsed_file.items():
        if len(current_time_series) >= 1000:
            network_threads.append(
                threading.Thread(
                    target=_log_error,
                    args=(client.datapoints.post_multi_time_series_datapoints, current_time_series[:]),
                )
            )

            current_time_series.clear()

        name = col_name.rpartition(":")[2].strip()
        external_id = col_name.rpartition(":")[0].strip()

        if external_id not in existing_time_series:
            create_time_series(client, name, external_id)
            existing_time_series[external_id] = name
            monitor.incr_created_time_series_counter()

        data_points = create_data_points(v[1:], timestamps)
        if data_points:
            current_time_series.append(
                TimeseriesWithDatapoints(name=existing_time_series[external_id], datapoints=data_points)
            )
            count_of_data_points += len(data_points)
            unique_external_ids.add(external_id)

    if current_time_series:
        network_threads.append(
            threading.Thread(
                target=_log_error, args=(client.datapoints.post_multi_time_series_datapoints, current_time_series)
            )
        )

    logger.info("Time to process file {}: {:.2f} seconds".format(csv_path, time.time() - start_time))

    return network_threads, count_of_data_points, len(unique_external_ids)


def post_all_data(queue, monitor, finished_path):
    start_time = time.time()

    all_threads = list(chain(*map(itemgetter(0), queue)))
    for thread in all_threads:
        thread.start()
    for thread in all_threads:
        thread.join()

    for path in map(itemgetter(1), queue):
        try:
            if finished_path is None:
                path.unlink()
            else:
                path.replace(finished_path.joinpath(path.name))
        except IOError as exc:
            logger.debug("Unable to delete file {}: {!s}".format(path, exc))

    monitor.incr_total_data_points_counter(sum(map(itemgetter(2), queue)))

    logger.info("Total time to send batch of request: {:.2f} seconds".format(time.time() - start_time))


def process_files(client, monitor, paths, time_series_cache, failed_path, finished_path) -> None:
    """Process one csv file at a time, and either delete it or move it when done."""
    monitor.successfully_processed_files_gauge.set(0)
    monitor.unprocessed_files_gauge.set(len(paths))
    thread_queue = []
    start_time = time.time()

    for path in paths:
        try:
            threads, data_points_count, time_series_count = process_csv_file(client, monitor, path, time_series_cache)
            thread_queue.append((threads, path, data_points_count))
        except IOError as exc:
            logger.debug("Unable to open file {}: {!s}".format(path, exc))
        except Exception as exc:
            logger.error("Parsing of file {} failed: {!s}".format(path, exc), exc_info=exc)
            monitor.incr_failed_files_counter()

            if failed_path is not None:
                path.replace(failed_path.joinpath(path.name))

        else:
            monitor.successfully_processed_files_gauge.inc()
            monitor.count_of_time_series_gauge.set(time_series_count)

            if len(thread_queue) >= 20:
                post_all_data(thread_queue, monitor, finished_path)
                thread_queue.clear()

        monitor.unprocessed_files_gauge.dec()
        monitor.push()

    if thread_queue:
        post_all_data(thread_queue, monitor, finished_path)
        monitor.push()

    logger.info("Total time to process {} of files: {:.2f} seconds".format(len(paths), time.time() - start_time))


def find_files_in_path(folder_path):
    """Return csv files in 'folder_path' sorted by newest first on last modified timestamp of files."""
    before_timestamp = time.time() - 1.0  # Only process files older than 1 seconds
    all_relevant_paths = []

    for path in folder_path.glob("*.csv"):
        try:
            modified_timestamp = path.stat().st_mtime
        except IOError as exc:  # Possible that file no longer exists, multiple extractors
            logger.debug("Failed to find stats on file {!s}: {!s}".format(path, exc))
            continue
        if modified_timestamp < before_timestamp:
            all_relevant_paths.append((path, modified_timestamp))

    return [p for p, _ in sorted(all_relevant_paths, key=itemgetter(1), reverse=True)]
