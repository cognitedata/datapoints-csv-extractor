import argparse
import logging
import os
import sys
import time
from logging.handlers import TimedRotatingFileHandler
from operator import itemgetter
from pathlib import Path

import pandas
from cognite import CogniteClient, APIError
from cognite.client.stable.datapoints import Datapoint, TimeseriesWithDatapoints
from cognite.client.stable.time_series import TimeSeries

logger = logging.getLogger(__name__)

API_KEY = os.environ.get("COGNITE_EXTRACTOR_API_KEY")
if not API_KEY:
    print("COME ON JAN&SAM, YOU FORGOT THE API KEY!")
    sys.exit(2)


# Global variable for last timestamp processed
LAST_PROCESSED_TIMESTAMP = 1550076300

# Maximum number of time series batched at once
BATCH_MAX = 1000


def get_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument("-l", "--live", action='store_true', \
        help="By default, historical data will be processed. Use '-l' tag to process live data. \
        If live data, the earliest time stamp to examine must be specified.")
    parser.add_argument("-p", "--folder", required=True, help="Folder path of the files processed")
    parser.add_argument("-o", "--log", required=True, help="Folder path of logfile")
    return parser


def configure_logger(data_type, log_path):
    log_file = os.path.join(log_path, f"extractor-{data_type}.log")
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s - %(message)s",
        handlers=[
            TimedRotatingFileHandler(log_file, when="midnight", backupCount=7),
            logging.StreamHandler(sys.stdout),
        ],
    )


def post_datapoints(client, paths, existing_time_series):
    current_time_series = []  # List of time series being processed

    def post_datapoints_request():
        nonlocal current_time_series
        client.datapoints.post_multi_time_series_datapoints(current_time_series)
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
                        current_time_series.append(TimeseriesWithDatapoints(name=existing_time_series[external_id], datapoints=datapoints))
                        count_of_datapoints += len(datapoints)

                else:
                    new_time_series = TimeSeries(name=name, description="Auto-generated time series attached to Placeholder asset, external ID not found", metadata={'externalID': external_id})
                    client.time_series.post_time_series([new_time_series])
                    existing_time_series[external_id] = name

                    datapoints = create_datapoints(df[col], timestamps)

                    if datapoints:
                        current_time_series.append(TimeseriesWithDatapoints(name=name, datapoints=datapoints))
                        count_of_datapoints += len(datapoints)

            if current_time_series:
                post_datapoints_request()

            logger.info("Processed {} datapoints from {}".format(count_of_datapoints, path))

    for path in paths:
        process_data(path)

    return max(path.stat().st_mtime for path in paths)  # Timestamp of most recent modified path


def find_new_files(last_mtime, base_path):
    paths = [(p, p.stat().st_mtime) for p in Path(base_path).glob("*.csv")]
    paths.sort(key=itemgetter(1), reverse=True)  # Process newest file first
    t_minus_2 = int(time.time()-2) # Process files more than 2 seconds old
    return [p for p, mtime in paths if mtime > last_mtime and mtime < t_minus_2]


def extract_datapoints(client, existing_time_series, data_type, folder_path):
    try:
        client.login.status()
    except APIError as error:
        logger.warning(error)
        client = CogniteClient(api_key=API_KEY)

    try:
        if data_type == "live":
            last_timestamp = LAST_PROCESSED_TIMESTAMP

            while True:
                paths = find_new_files(last_timestamp, folder_path)
                if paths:
                    last_timestamp = post_datapoints(client, paths, existing_time_series)

                    # logger.info("Removing processed files {}".format(', '.join(p.name for p in paths)))
                    # for path in paths:
                    #    path.unlink()

                    time.sleep(5)
        elif data_type == "historical":
            paths = find_new_files(0, folder_path) # All paths in folder, regardless of timestamp
            if paths:
                post_datapoints(client, paths, existing_time_series)
            logger.info("Extraction complete")
    except KeyboardInterrupt:
        logger.warning("Extractor stopped")


if __name__ == "__main__":

    # Parse command line arguments
    parser = get_parser()
    args = parser.parse_args()

    data_type = "live" if args.live else "historical"

    # Configure logger
    configure_logger(data_type, args.log)

    # Establish API connection and get initial dictionary of existing time series
    client = CogniteClient(api_key=API_KEY)
    existing_time_series = {i["metadata"]["externalID"]:i["name"] for i in client.time_series.get_time_series(include_metadata=True, autopaging=True).to_json()}

    extract_datapoints(client, existing_time_series, data_type, args.folder)
