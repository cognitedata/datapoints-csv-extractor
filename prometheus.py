from cognite_prometheus.cognite_prometheus import CognitePrometheus
from prometheus_client import Counter


class Prometheus:
    def __init__(self, prometheus, live: bool):
        self.prometheus = prometheus
        self.data_type = "live" if live else "historical"

        self.time_series_counter = Counter(
            "time_series_created_total",
            "Number of time series created since the extractor started running",
            labelnames=["data_type"],
            registry=CognitePrometheus.registry,
        )

        self.all_data_points_counter = Counter(
            "data_points_posted_total",
            "Number of datapoints posted since the extractor started running",
            labelnames=["data_type"],
            registry=CognitePrometheus.registry,
        )

        self.time_series_data_points_counter = Counter(
            "data_points_posted_per_time_series",
            "Number of datapoints posted per time series (based on external ID) since the extractor started running",
            labelnames=["data_type", "external_id"],
            registry=CognitePrometheus.registry,
        )
