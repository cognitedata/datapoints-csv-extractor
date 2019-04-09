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
from cognite.client import APIError, CogniteClient

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
        "--historical", default=True, action="store_true", help="Process historical data instead of live"
    )
    parser.add_argument("--input", "-i", required=True, help="Folder path of the files to process")
    parser.add_argument("--timestamp", "-t", required=False, type=int, help="Optional, process files older than this")
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


def _configure_logger(folder_path, live_processing: bool) -> None:
    """Create 'folder_path' and configure logging to file as well as console."""
    name_postfix = "live" if live_processing else "historical"
    folder_path.mkdir(parents=True, exist_ok=True)
    log_file = folder_path.joinpath("extractor-{}.log".format(name_postfix))
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s - %(message)s",
        handlers=[
            TimedRotatingFileHandler(log_file, when="midnight", backupCount=7),
            logging.StreamHandler(sys.stdout),
        ],
    )

    if os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"):  # Temp hack
        google.cloud.logging.Client().setup_logging(name="csv-extractor-{}".format(name_postfix))


def main(args):
    _configure_logger(Path(args.log), args.live)

    api_key = args.api_key if args.api_key else os.environ.get("COGNITE_EXTRACTOR_API_KEY")
    args.api_key = ""  # Don't log the api key if given through CLI
    logger.info("Extractor configured with {}".format(args))
    start_timestamp = args.timestamp if args.timestamp else 0

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

    project_name = client._project
    monitor = configure_prometheus(args.live, project_name) 

    extract_data_points(
        client, monitor, get_all_time_series(client), args.live, start_timestamp, input_path, failed_path
    )


if __name__ == "__main__":
    main(_parse_cli_args())
