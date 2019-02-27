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
    parser.add_argument("--failed", "-f", required=False, default="failed", help="Where to put inputs that failed")
    parser.add_argument("--log", "-d", required=False, default="log", help="Optional, log directory")
    parser.add_argument("--apikey", "-k", required=False, help="Optional, CDP API KEY")
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


def post_request(endpoint, parameter):
    try:
        endpoint(parameter)
    except Exception as error:
        logger.info(error)


def post_datapoints(client, paths, existing_time_series, failed_path):
    current_time_series = []  # List of time series being processed

    def post_datapoints_request():
        nonlocal current_time_series
        post_request(client.datapoints.post_multi_time_series_datapoints, current_time_series)
        current_time_series = []

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
        except FileNotFoundError as file_error:
            logger.info(file_error)
        except ValueError as format_error:
            logger.info(format_error)
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

    def process_data(path):
        nonlocal current_time_series
        nonlocal existing_time_series
        df = parse_csv(path)
        if df is not None:
            timestamps = [int(o) * 1000 for o in df.index.tolist()]
            count_of_datapoints = 0

            for col in df:
                if len(current_time_series) >= BATCH_MAX:
                    post_datapoints_request()

                name = str(col.rpartition(":")[2].strip())
                external_id = str(col.rpartition(":")[0].strip())

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
                    post_request(client.time_series.post_time_series, [new_time_series])
                    existing_time_series[external_id] = name

                    datapoints = create_datapoints(df[col], timestamps)

                    if datapoints:
                        current_time_series.append(TimeseriesWithDatapoints(name=name, datapoints=datapoints))
                        count_of_datapoints += len(datapoints)

            if current_time_series:
                post_datapoints_request()

            logger.info("Processed {} datapoints from {}".format(count_of_datapoints, path))

    for path in paths:
        try:
            try:
                process_data(path)
            except Exception as exc:
                logger.error("Parsing of file {} failed: {!s}".format(path, exc))
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


def extract_datapoints(client, existing_time_series, process_live_data: bool, folder_path, failed_path):
    try:
        if process_live_data:
            last_timestamp = LAST_PROCESSED_TIMESTAMP
            while True:
                paths = find_new_files(last_timestamp, folder_path)
                if paths:
                    last_timestamp = max(path.stat().st_mtime for path in paths)  # Timestamp of most recent modified
                    post_datapoints(client, paths, existing_time_series, failed_path)
                time.sleep(5)

        else:
            paths = find_new_files(0, folder_path)  # All paths in folder, regardless of timestamp
            if paths:
                post_datapoints(client, paths, existing_time_series, failed_path)
            else:
                logger.info("Found no files to process in {}".format(folder_path))
        logger.info("Extraction complete")
    except KeyboardInterrupt:
        logger.warning("Extractor stopped")


def main(args):
    _configure_logger(args.log, args.live)
    api_key = args.apikey if args.apikey else os.environ.get("COGNITE_EXTRACTOR_API_KEY")
    args.apikey = ""  # Don't log the api key if given through CLI
    logger.info("Extractor configured with {}".format(args))

    # Establish API connection and get initial dictionary of existing time series
    try:
        client = CogniteClient(api_key=api_key)
        client.login.status()
    except APIError as exc:
        logger.error("Failed to create CDP client: {!s}".format(exc))
        client = CogniteClient(api_key=api_key)

    existing_time_series = {
        i["metadata"]["externalID"]: i["name"]
        for i in client.time_series.get_time_series(include_metadata=True, autopaging=True).to_json()
    }

    extract_datapoints(client, existing_time_series, args.live, Path(args.input), Path(args.failed))


if __name__ == "__main__":
    main(_parse_cli_args())
