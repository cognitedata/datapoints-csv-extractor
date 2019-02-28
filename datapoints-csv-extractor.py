import argparse
import logging
import os
import sys
import time
from logging.handlers import TimedRotatingFileHandler
from operator import itemgetter
from pathlib import Path

import pandas
from cognite import APIError, CogniteClient
from cognite.client.stable.datapoints import Datapoint, TimeseriesWithDatapoints
from cognite.client.stable.time_series import TimeSeries

logger = logging.getLogger(__name__)

# Global variable for last timestamp processed
LAST_PROCESSED_TIMESTAMP = 1_551_265_200

# Maximum number of time series batched at once
BATCH_MAX = 1000


def _parse_cli_args():
    parser = argparse.ArgumentParser()
    group = parser.add_mutually_exclusive_group()
    group.add_argument(
        "--live",
        "-l",
        action="store_true",
        help="By default, historical data will be processed. Use '-l' tag to process live data. \
        If live data, the earliest time stamp to examine must be specified.",
    )
    group.add_argument(
        "--historical", default=True, action="store_true", help="Process historical data instead of live"
    )
    parser.add_argument("--input", "-i", required=True, help="Folder path of the files to process")
    parser.add_argument("--log", "-d", required=False, default="log", help="Optional, log directory")
    parser.add_argument(
        "--move-failed",
        "-m",
        required=False,
        action="store_true",
        help="Optional, move failed csv files to subfolder failed",
    )
    parser.add_argument("--api-key", "-k", required=False, help="Optional, CDP API KEY")
    return parser.parse_args()


def _configure_logger(folder_path, live_processing):
    os.makedirs(folder_path, exist_ok=True)
    log_file = os.path.join(folder_path, "extractor-{}.log".format("live" if live_processing else "historical"))
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s - %(message)s",
        handlers=[
            TimedRotatingFileHandler(log_file, when="midnight", backupCount=7),
            logging.StreamHandler(sys.stdout),
        ],
    )


def _log_error(endpoint, parameter):
    try:
        endpoint(parameter)
    except Exception as error:
        logger.info(error)


def process_files(client, paths, time_series_cache, failed_path):
    def convert_float(value_string):
        try:
            value_float = float(value_string.replace(",", "."))
        except ValueError as error:
            logger.info(error)
        else:
            return value_float

    def parse_csv(csv_path):
        try:
            df = pandas.read_csv(csv_path, encoding="latin-1", delimiter=";", quotechar='"', skiprows=[1], index_col=0)
        except IOError as file_error:
            logger.warning(file_error)
        except ValueError as format_error:
            logger.warning(format_error)
        else:
            return df

    def create_datapoints(col_values, timestamps):
        datapoints = []

        for i, value in enumerate(col_values.tolist()):
            if pandas.notnull(value):
                value = convert_float(value)
                if value is not None:
                    datapoints.append(Datapoint(timestamp=timestamps[i], value=value))

        return datapoints

    def process_data(csv_path, existing_time_series):
        current_time_series = []  # List of time series being processed
        df = parse_csv(csv_path)

        if df is not None:
            timestamps = [int(o) * 1000 for o in df.index.tolist()]
            count_of_datapoints = 0

            for col in df:
                if len(current_time_series) >= BATCH_MAX:
                    _log_error(client.datapoints.post_multi_time_series_datapoints, current_time_series)
                    current_time_series = []

                name = col.rpartition(":")[2].strip()
                external_id = col.rpartition(":")[0].strip()

                if external_id in existing_time_series:
                    datapoints = create_datapoints(df[col], timestamps)

                    if datapoints:
                        current_time_series.append(
                            TimeseriesWithDatapoints(name=existing_time_series[external_id], datapoints=datapoints)
                        )
                        count_of_datapoints += len(datapoints)

                else:
                    new_time_series = TimeSeries(
                        name=name,
                        description="Auto-generated time series attached to Placeholder asset, external ID not found",
                        metadata={"externalID": external_id},
                    )
                    _log_error(client.time_series.post_time_series, [new_time_series])
                    existing_time_series[external_id] = name

                    datapoints = create_datapoints(df[col], timestamps)

                    if datapoints:
                        current_time_series.append(TimeseriesWithDatapoints(name=name, datapoints=datapoints))
                        count_of_datapoints += len(datapoints)

            if current_time_series:
                _log_error(client.datapoints.post_multi_time_series_datapoints, current_time_series)

            logger.info("Processed {} datapoints from {}".format(count_of_datapoints, path))

    for path in paths:
        try:
            try:
                process_data(path, time_series_cache)
            except Exception as exc:
                logger.error("Parsing of file {} failed: {!s}".format(path, exc))
                if failed_path is not None:
                    failed_path.mkdir(parents=True, exist_ok=True)
                    path.replace(failed_path.joinpath(path.name))
            else:
                path.unlink()
        except IOError as exc:
            logger.error("Failed to delete/move file {}: {!s}".format(path, exc))


def find_new_files(last_mtime, base_path):
    all_paths = [(p, p.stat().st_mtime) for p in base_path.glob("*.csv")]
    all_paths.sort(key=itemgetter(1), reverse=True)  # Process newest file first
    t_minus_2 = int(time.time() - 2)  # Process files more than 2 seconds old
    return [p for p, mtime in all_paths if last_mtime < mtime < t_minus_2]


def get_all_time_series(client):
    """Get map of timeseries externalId to name of all timeseries that has externalId."""
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

    return {i["metadata"]["externalID"]: i["name"] for i in res.to_json() if "externalID" in i["metadata"]}


def extract_data_points(client, time_series_cache, process_live_data: bool, folder_path, failed_path):
    try:
        if process_live_data:
            last_timestamp = LAST_PROCESSED_TIMESTAMP
            while True:
                paths = find_new_files(last_timestamp, folder_path)
                if paths:
                    last_timestamp = max(path.stat().st_mtime for path in paths)  # Timestamp of most recent modified
                    process_files(client, paths[:20], time_series_cache, failed_path)  # Only 20 most recent
                time.sleep(5)

        else:
            paths = find_new_files(0, folder_path)  # All paths in folder, regardless of timestamp
            if paths:
                process_files(client, paths, time_series_cache, failed_path)
            else:
                logger.info("Found no files to process in {}".format(folder_path))
        logger.info("Extraction complete")
    except KeyboardInterrupt:
        logger.warning("Extractor stopped")


def main(args):
    _configure_logger(args.log, args.live)

    api_key = args.api_key if args.api_key else os.environ.get("COGNITE_EXTRACTOR_API_KEY")
    args.api_key = ""  # Don't log the api key if given through CLI
    logger.info("Extractor configured with {}".format(args))

    input_path = Path(args.input)
    if not input_path.exists():
        logger.fatal("Input folder does not exists: {!s}".format(input_path))
        sys.exit(2)
    failed_path = input_path.joinpath("failed") if args.move_failed else None

    try:
        client = CogniteClient(api_key=api_key)
        client.login.status()
    except APIError as exc:
        logger.error("Failed to create CDP client: {!s}".format(exc))
        client = CogniteClient(api_key=api_key)

    extract_data_points(client, get_all_time_series(client), args.live, input_path, failed_path)


if __name__ == "__main__":
    main(_parse_cli_args())
