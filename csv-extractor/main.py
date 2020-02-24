#!/usr/bin/env python
# coding: utf-8
"""
A script that process csv files in a specified folder,
to extract data points to send to CDP.
"""
import argparse
import logging
import os
import sys
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path

import google.cloud.logging
from cognite.client import CogniteClient
from cognite.client.exceptions import CogniteAPIError

from csv_extractor import extract_data_points, get_all_time_series
from monitoring import configure_prometheus

logger = logging.getLogger(__name__)


def _parse_cli_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser()
    group = parser.add_mutually_exclusive_group()
    group.add_argument(
        "--live",
        "-l",
        action="store_true",
        help="By default, historical data will be processed. Use '--live' to process live data",
    )
    group.add_argument(
        "--historical",
        default=True,
        action="store_true",
        help="Process historical data instead of live",
    )
    parser.add_argument("--api-key", "-k", required=False, help="Optional, CDP API KEY")
    parser.add_argument(
        "--input", "-i", required=True, help="Folder path of the files to process"
    )
    parser.add_argument(
        "--log", "-d", required=False, default="log", help="Optional, log directory"
    )
    parser.add_argument(
        "--log-level", required=False, default="INFO", help="Optional, logging level"
    )
    parser.add_argument(
        "--move-failed",
        required=False,
        action="store_true",
        help="Optional, move failed csv files to subfolder failed",
    )
    parser.add_argument(
        "--keep-finished",
        required=False,
        action="store_true",
        help="Optional, move successful csv files to subfolder finished",
    )
    parser.add_argument(
        "--from-time", required=False, type=int, help="Optional, only process if older"
    )
    parser.add_argument(
        "--until-time",
        required=False,
        type=int,
        help="Optional, only process if younger",
    )

    return parser.parse_args()


def _configure_logger(folder_path, live_processing: bool, log_level: str) -> None:
    """Create 'folder_path' and configure logging to file as well as console."""
    name_postfix = "live" if live_processing else "historical"
    folder_path.mkdir(parents=True, exist_ok=True)
    log_file = folder_path.joinpath("extractor-{}.log".format(name_postfix))
    logging.basicConfig(
        level=logging.INFO if log_level == "INFO" else log_level,
        format="%(asctime)s %(name)s %(levelname)s - %(message)s",
        handlers=[
            TimedRotatingFileHandler(log_file, when="midnight", backupCount=7),
            logging.StreamHandler(sys.stdout),
        ],
    )

    if os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"):  # Temp hack
        google.cloud.logging.Client().setup_logging(
            name="csv-extractor-{}".format(name_postfix)
        )


def _convert_timestamp_maybe(timestamp_str):
    if timestamp_str:
        return
    return


def main(args):
    _configure_logger(Path(args.log), args.live, args.log_level)

    api_key = (
        args.api_key if args.api_key else os.environ.get("COGNITE_EXTRACTOR_API_KEY")
    )
    args.api_key = ""  # Don't log the api key if given through CLI
    logger.info("Extractor configured with {}".format(args))

    input_path = Path(args.input)
    if not input_path.exists():
        logger.fatal("Input folder does not exists: {!s}".format(input_path))
        sys.exit(2)

    failed_path = input_path.joinpath("failed") if args.move_failed else None
    finished_path = input_path.joinpath("finished") if args.keep_finished else None
    if failed_path:
        failed_path.mkdir(parents=True, exist_ok=True)
    if finished_path:
        finished_path.mkdir(parents=True, exist_ok=True)

    try:
        client = CogniteClient(
            api_key=api_key, client_name="tebis-csv-datapoint-extractor"
        )
        client.login.status()
    except CogniteAPIError as exc:
        logger.error("Failed to create CDP client: {!s}".format(exc))
        client = CogniteClient(api_key=api_key)

    project_name = client.config.project
    monitor = configure_prometheus(args.live, project_name)

    try:
        extract_data_points(
            client,
            monitor,
            get_all_time_series(client),
            args.live,
            args.from_time,
            args.until_time,
            input_path,
            failed_path,
            finished_path,
        )
    except KeyboardInterrupt:
        logger.warning("Extractor stopped")


if __name__ == "__main__":
    main(_parse_cli_args())
