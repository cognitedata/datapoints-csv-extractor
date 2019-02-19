import argparse
import logging
import os
import sys
import time
from logging.handlers import TimedRotatingFileHandler
from operator import itemgetter
from pathlib import Path

import pandas
from cognite import CogniteClient
from cognite.client.stable.datapoints import Datapoint, TimeseriesWithDatapoints

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(name)s %(levelname)s - %(message)s",
    handlers=[
        TimedRotatingFileHandler("extractor.log", when="midnight", backupCount=7),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger()


API_KEY = os.environ.get("COGNITE_EXTRACTOR_API_KEY")
if not API_KEY:
    print("COME ON JAN&SAM, YOU FORGOT THE API KEY!")
    sys.exit(2)


# Global variable for last timestamp processed
LAST_PROCESSED_TIMESTAMP = 1550076300

# Maximum number of time series batched at once
BATCH_MAX = 1000

# Path to folder of CSV files
FOLDER_PATH = "../TebisSampleData2/"


def get_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument('-d', '--data_type', choices=['live', 'historical'], type=str.lower, \
        help='Input should be "live" or "historical" to specify data type. \
        If live data, the earliest time stamp to examine must be specified.')
    return parser


def post_datapoints(client, paths, existing_timeseries):
    current_time_series = []  # List of time series being processed

    def post_datapoints():
        nonlocal current_time_series
        client.datapoints.post_multi_time_series_datapoints(current_time_series)
        current_time_series = []

    def convert_float(value_str):
        try:
            value_str = float(value_str.replace(",", "."))
        except ValueError as error:
            logger.info(error)
        else:
            return value_str

    def parse_csv(csv_path):
        try:
            df = pandas.read_csv(csv_path, encoding="latin-1", delimiter=";", quotechar='"', skiprows=[1], index_col=0)
        except FileNotFoundError as file_error:
            logger.info(file_error)
        except ValueError as format_error:
            logger.info(format_error)
        else:
            return df

    for path in paths:
        df = parse_csv(path)
        if df is not None:
            timestamps = [int(o) * 1000 for o in df.index.tolist()]
            count_of_data_points = 0

            for col in df:
                if len(current_time_series) >= BATCH_MAX:
                    post_datapoints()

                name = str(col.rpartition(":")[2].strip())
                external_id = str(col.rpartition(":")[0].strip())

                if external_id in existing_timeseries:
                    data_points = []

                    for i, value in enumerate(df[col].tolist()):
                        if pandas.notnull(value):
                            value = convert_float(value)
                            if value is not None:
                                data_points.append(Datapoint(timestamp=timestamps[i], value=value))

                    if data_points:
                        current_time_series.append(TimeseriesWithDatapoints(name=existing_timeseries[external_id], datapoints=data_points))
                        print(existing_timeseries[external_id], external_id)
                        count_of_data_points += len(data_points)

            if current_time_series:
                post_datapoints()

            logger.info("Processed {} datapoints from {}".format(count_of_data_points, path))

    return max(path.stat().st_mtime for path in paths)  # Timestamp of most recent modified path


def find_new_files(last_mtime, base_path):
    paths = [(p, p.stat().st_mtime) for p in Path(base_path).glob("*.csv")]
    paths.sort(key=itemgetter(1), reverse=True)  # Process newest file first
    t_minus_2 = int(time.time()-2) # Process files more than 2 seconds old
    return [p for p, mtime in paths if mtime > last_mtime and mtime < t_minus_2]


def extract_datapoints(data_type):
    client = CogniteClient(api_key=API_KEY)
    existing_timeseries = {i["metadata"]["externalID"]:i["name"] for i in client.time_series.get_time_series(include_metadata=True, autopaging=True).to_json()}

    if data_type == 'live':
        try:
            while True:
                paths = find_new_files(last_timestamp, FOLDER_PATH)
                if paths:
                    last_timestamp = post_datapoints(client, paths, existing_timeseries)

                    # logger.info("Removing processed files {}".format(', '.join(p.name for p in paths)))
                    # for path in paths:
                    #    path.unlink()

                    time.sleep(5)
        except KeyboardInterrupt:
            logger.warning("Extractor stopped")
    elif data_type == 'historical':
        paths = find_new_files(0, FOLDER_PATH) # All paths in folder, regardless of timestamp
        if paths:
            post_datapoints(client, paths, existing_timeseries)
        logger.info("Extraction complete")


if __name__ == "__main__":

    # Parse command line arguments
    parser = get_parser()
    args = parser.parse_args()

    extract_datapoints(args.data_type)
