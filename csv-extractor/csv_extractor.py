# coding: utf-8
"""
A module for extracting datapoints from CSV files.
"""
import asyncio
import csv
import logging
import sys
import time
from collections import defaultdict
from operator import itemgetter

from cognite.client import APIError
from cognite.client.stable.datapoints import Datapoint, TimeseriesWithDatapoints
from cognite.client.stable.time_series import TimeSeries

import aiofiles
import aiofiles.os

logger = logging.getLogger(__name__)


BATCH_MAX = 1000  # Maximum number of time series batched at once


async def extract_data_points(
    client, monitor, time_series_cache, live_mode: bool, folder_path, failed_path, finished_path
):
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
            await process_files(files, client, monitor, time_series_cache, failed_path, finished_path)

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


async def process_files(paths, client, monitor, time_series_cache, failed_path, finished_path, batch_size=10):
    """Process a batch of csv file at a time, and either delete or move them when done."""
    monitor.successfully_processed_files_gauge.set(0)
    monitor.unprocessed_files_gauge.set(len(paths))

    for i in range(0, len(paths), batch_size):
        start_time = time.time()

        futures = []
        for path in paths[i : i + batch_size]:
            futures.append(process_one_file(path, client, time_series_cache, monitor, failed_path, finished_path))
        await asyncio.gather(*futures, return_exceptions=True)

        logger.info("Total time to process {} files: {:.2f} seconds".format(len(futures), time.time() - start_time))


async def process_one_file(path, client, time_series_cache, monitor, failed_path, finished_path):
    """Read and understand content of one csv file, then send data points to CDF."""
    start_time = time.time()

    try:
        content = await read_and_parse_csv_file(path)

        data_points, new_time_series, data_points_count, time_series_count = convert_file_content_to_data_points(
            content, time_series_cache
        )
        if new_time_series:
            _log_error(client.time_series.post_time_series, new_time_series)
            monitor.incr_created_time_series_counter(len(new_time_series))

        _log_error(client.datapoints.post_multi_time_series_datapoints, data_points)

        if finished_path is None:
            await aiofiles.os.remove(path)
        else:
            await aiofiles.os.rename(path, finished_path.joinpath(path.name))

        logger.info("Time to process file {}: {:.2f} seconds".format(path, time.time() - start_time))

    except IOError as exc:
        logger.debug("Unable to open or delete file {}: {!s}".format(path, exc))
    except Exception as exc:
        logger.error("Parsing of file {} failed: {!s}".format(path, exc), exc_info=exc)
        monitor.incr_failed_files_counter()
        if failed_path is not None:
            await aiofiles.os.rename(path, failed_path.joinpath(path.name))
    else:
        monitor.successfully_processed_files_gauge.inc()
        monitor.incr_total_data_points_counter(data_points_count)
        monitor.count_of_time_series_gauge.set(time_series_count)

    monitor.unprocessed_files_gauge.dec()
    monitor.push()


async def read_and_parse_csv_file(csv_path):
    """Parse the csv file and return the data in a {col_name -> list_of_row_items} dictionary"""
    parsed_file = defaultdict(list)
    async with aiofiles.open(csv_path, mode="r", encoding="latin-1") as f:
        lines = await f.readlines()
    for row in csv.DictReader(lines, delimiter=";"):
        for k, v in row.items():
            parsed_file[k].append(v)
    return parsed_file


def convert_file_content_to_data_points(content, existing_time_series):
    """Take content of a csv file and produce list of TimeseriesWithDatapoints"""
    timestamps = content[""][1:]  # ignore garbage value in first line
    del content[""]  # remove the timestamps from the dictionary

    time_series_with_data_points = []
    unknown_time_series = []
    unique_external_ids = set()  # Count number of time series processed
    count_of_data_points = 0

    for col_name, values in content.items():
        name, _, external_id = col_name.rpartition(":")
        name, external_id = name.strip(), external_id.strip()

        if external_id not in existing_time_series:
            existing_time_series[external_id] = name
            unknown_time_series.append(
                TimeSeries(
                    name=name,
                    description="Auto-generated time series, external ID not found",
                    metadata={"externalID": external_id},
                )
            )

        data_points = create_data_points(values[1:], timestamps)
        if data_points:
            time_series_with_data_points.append(
                TimeseriesWithDatapoints(name=existing_time_series[external_id], datapoints=data_points)
            )
            count_of_data_points += len(data_points)
            unique_external_ids.add(external_id)

    return time_series_with_data_points, unknown_time_series, count_of_data_points, len(unique_external_ids)


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
