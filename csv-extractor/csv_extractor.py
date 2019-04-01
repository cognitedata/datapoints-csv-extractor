# coding: utf-8
"""
A module for sending monitoring statistics to Prometheus.
"""
import logging
import sys
import time
from operator import itemgetter

import pandas
from cognite.client import APIError
from cognite.client.stable.datapoints import Datapoint, TimeseriesWithDatapoints
from cognite.client.stable.time_series import TimeSeries

logger = logging.getLogger(__name__)


BATCH_MAX = 1000  # Maximum number of time series batched at once


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
        if pandas.notnull(value_string):
            if not isinstance(value_string, str):
                value = value_string
            else:
                try:
                    value = float(value_string.replace(",", "."))
                except ValueError as error:
                    logger.info(error)
                    continue
            data_points.append(Datapoint(timestamp=timestamps[i], value=value))

    return data_points


def process_csv_file(client, monitor, csv_path, existing_time_series) -> None:
    """Find datapoints inside a single csv file and send it to CDP."""
    count_of_data_points = 0
    current_time_series = []  # List of time series being processed

    df = pandas.read_csv(csv_path, encoding="latin-1", delimiter=";", quotechar='"', skiprows=[1], index_col=0)
    timestamps = [int(o) * 1000 for o in df.index.tolist()]

    for col in df:
        if len(current_time_series) >= BATCH_MAX:
            _log_error(client.datapoints.post_multi_time_series_datapoints, current_time_series)
            current_time_series.clear()

        name = col.rpartition(":")[2].strip()
        external_id = col.rpartition(":")[0].strip()

        if external_id not in existing_time_series:
            new_time_series = TimeSeries(
                name=name,
                description="Auto-generated time series, external ID not found",
                metadata={"externalID": external_id},
            )
            _log_error(client.time_series.post_time_series, [new_time_series])
            existing_time_series[external_id] = name
            monitor.incr_time_series_counter()

        data_points = create_data_points(df[col].tolist(), timestamps)
        if data_points:
            current_time_series.append(
                TimeseriesWithDatapoints(name=existing_time_series[external_id], datapoints=data_points)
            )
            count_of_data_points += len(data_points)
            monitor.incr_data_points_counter(external_id, len(data_points))

    if current_time_series:
        _log_error(client.datapoints.post_multi_time_series_datapoints, current_time_series)

    logger.info("Processed {} datapoints from {}".format(count_of_data_points, csv_path))
    monitor.incr_total_data_points_counter(count_of_data_points)
    monitor.push()


def process_files(client, monitor, paths, time_series_cache, failed_path) -> None:
    """Process one csv file at a time, and either delete it or possibly move it when done."""
    for path in paths:
        try:
            try:
                process_csv_file(client, monitor, path, time_series_cache)
            except Exception as exc:
                logger.error("Parsing of file {} failed: {!s}".format(path, exc), exc_info=exc)
                if failed_path is not None:
                    failed_path.mkdir(parents=True, exist_ok=True)
                    path.replace(failed_path.joinpath(path.name))
            else:
                path.unlink()
        except IOError as exc:
            logger.warning("Failed to delete/move file {}: {!s}".format(path, exc))


def find_files_in_path(folder_path, after_timestamp: int, limit: int = None, newest_first: bool = True):
    """Return csv files in 'folder_path' sorted by 'newest_first' on last modified timestamp of files."""
    before_timestamp = int(time.time() - 2)  # Process files more than 2 seconds old
    all_relevant_paths = []

    for path in folder_path.glob("*.csv"):
        try:
            modified_timestamp = path.stat().st_mtime
        except IOError as exc:  # Possible that file no longer exists, multiple extractors
            logger.debug("Failed to find stats on file {!s}: {!s}".format(path, exc))
            continue
        if after_timestamp < modified_timestamp < before_timestamp:
            all_relevant_paths.append((path, modified_timestamp))

    paths = [p for p, _ in sorted(all_relevant_paths, key=itemgetter(1), reverse=newest_first)]
    return paths if not limit else paths[:limit]


def get_all_time_series(client):
    """Return map of timeseries externalId -> name of all timeseries that has externalId."""
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


def extract_data_points(
    client, monitor, time_series_cache, live_mode: bool, start_timestamp: int, folder_path, failed_path
):
    """Find datapoints in files in 'folder_path' and send them to CDP."""
    try:
        if live_mode:
            while True:
                paths = find_files_in_path(folder_path, start_timestamp, limit=20)
                if paths:
                    process_files(client, monitor, paths, time_series_cache, failed_path)
                time.sleep(3)

        else:
            paths = find_files_in_path(folder_path, start_timestamp, newest_first=False)
            if paths:
                process_files(client, monitor, paths, time_series_cache, failed_path)
            else:
                logger.info("Found no files to process in {}".format(folder_path))
        logger.info("Extraction complete")
    except KeyboardInterrupt:
        logger.warning("Extractor stopped")
