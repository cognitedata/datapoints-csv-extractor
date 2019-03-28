# coding: utf-8
"""
A module for sending monitoring statistics to Prometheus.
"""
import logging
import os
import socket

from cognite_prometheus.cognite_prometheus import CognitePrometheus
from prometheus_client import Counter, Info, PlatformCollector, ProcessCollector

logger = logging.getLogger(__name__)


def configure_prometheus(live: bool):
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
    return Prometheus(CognitePrometheus.get_prometheus_object(), live)


def _get_host_info():
    return {"hostname": socket.gethostname(), "fqdn": socket.getfqdn()}


class Prometheus:
    def __init__(self, prometheus, live: bool):
        self.prometheus = prometheus
        self.data_type = "live" if live else "historical"

        self.info = Info("host_info", "Host info", registry=CognitePrometheus.registry)
        self.info.info(_get_host_info())
        self.process = ProcessCollector(registry=CognitePrometheus.registry)
        self.platform = PlatformCollector(registry=CognitePrometheus.registry)

        self.time_series_counter = Counter(
            "created_time_series_total",
            "Number of time series created since the extractor started running",
            labelnames=["data_type"],
            registry=CognitePrometheus.registry,
        )

        self.all_data_points_counter = Counter(
            "posted_data_points_total",
            "Number of datapoints posted since the extractor started running",
            labelnames=["data_type"],
            registry=CognitePrometheus.registry,
        )

        self.time_series_data_points_counter = Counter(
            "posted_data_points_per_time_series_total",
            "Number of datapoints posted per time series (based on external ID) since the extractor started running",
            labelnames=["data_type", "external_id"],
            registry=CognitePrometheus.registry,
        )

    def incr_time_series_counter(self, amount: int = 1) -> None:
        self.time_series_counter.labels(data_type=self.data_type).inc(amount)

    def incr_total_data_points_counter(self, amount: int) -> None:
        self.all_data_points_counter.labels(data_type=self.data_type).inc(amount)

    def incr_data_points_counter(self, external_id: str, amount: int) -> None:
        self.time_series_data_points_counter.labels(data_type=self.data_type, external_id=external_id).inc(amount)

    def push(self):
        try:
            self.prometheus.push_to_server()
        except Exception as exc:
            logger.error("Failed to push prometheus data: {!s}".format(exc))
