# coding: utf-8
"""
A module for sending monitoring statistics to Prometheus.
"""
import logging
import os
import socket

from cognite_prometheus.cognite_prometheus import CognitePrometheus
from prometheus_client import Counter, Gauge, Info, PlatformCollector, ProcessCollector

logger = logging.getLogger(__name__)


def configure_prometheus(live: bool, project_name):
    """Configure prometheus object, or return dummy object if not configured."""
    jobname = os.environ.get("COGNITE_PROMETHEUS_JOBNAME")
    username = os.environ.get("COGNITE_PROMETHEUS_USERNAME")
    password = os.environ.get("COGNITE_PROMETHEUS_PASSWORD")

    if not jobname or not username or not password:
        logger.warning("Prometheus is not configured: {} {}".format(jobname, username))
        unconfigured_dummy = True
    else:
        unconfigured_dummy = False

    try:
        CognitePrometheus(jobname, username, password, unconfigured_dummy=unconfigured_dummy)
    except Exception as exc:
        logger.error("Failed to create Prometheus object: {!s}".format(exc))
    return Prometheus(CognitePrometheus.get_prometheus_object(), live, project_name)


class Prometheus:
    labels = ["data_type", "project_name"]

    def __init__(self, prometheus, live: bool, project_name: str):
        self.project_name = project_name
        self.prometheus = prometheus
        self.data_type = "live" if live else "historical"
        self.namespace = f"csv_live" if live else f"csv_hist"
        self.label_values = (self.data_type, self.project_name)

        self.info = Info("host", "Host info", namespace=self.namespace, registry=CognitePrometheus.registry)
        self.info.info({"hostname": socket.gethostname(), "fqdn": socket.getfqdn()})
        self.process = ProcessCollector(namespace=self.namespace, registry=CognitePrometheus.registry)
        self.platform = PlatformCollector(registry=CognitePrometheus.registry)

        self.created_time_series_counter = Counter(
            "created_time_series_total",
            "Number of time series created since the extractor started running",
            namespace=self.namespace,
            labelnames=self.labels,
            registry=CognitePrometheus.registry,
        ).labels(*self.label_values)

        self.all_data_points_counter = Counter(
            "posted_data_points_total",
            "Number of datapoints posted since the extractor started running",
            namespace=self.namespace,
            labelnames=self.labels,
            registry=CognitePrometheus.registry,
        ).labels(*self.label_values)

        self.count_of_time_series_gauge = Gauge(
            "posted_time_series_count",
            "The number of timeseries that had valid datapoints in the current file",
            namespace=self.namespace,
            labelnames=self.labels,
            registry=CognitePrometheus.registry,
        ).labels(*self.label_values)

        self.available_csv_files_gauge = Gauge(
            "available_csv_files",
            "Number of csv files in the folder that could be processed by the extractor",
            namespace=self.namespace,
            labelnames=self.labels,
            registry=CognitePrometheus.registry,
        ).labels(*self.label_values)

        self.unprocessed_files_gauge = Gauge(
            "unprocessed_files",
            "Number of csv files that remains to be processed in this batch",
            namespace=self.namespace,
            labelnames=self.labels,
            registry=CognitePrometheus.registry,
        ).labels(*self.label_values)

        self.successfully_processed_files_gauge = Gauge(
            "successfully_processed_files",
            "Number of csv files that has been successfully processed in this batch",
            namespace=self.namespace,
            labelnames=self.labels,
            registry=CognitePrometheus.registry,
        ).labels(*self.label_values)

    def incr_created_time_series_counter(self, amount: int = 1) -> None:
        self.created_time_series_counter.inc(amount)

    def incr_total_data_points_counter(self, amount: int) -> None:
        self.all_data_points_counter.inc(amount)

    def push(self):
        try:
            self.prometheus.push_to_server()
        except Exception as exc:
            logger.error("Failed to push prometheus data: {!s}".format(exc))
