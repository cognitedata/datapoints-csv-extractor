from prometheus_client import Gauge

class Prometheus:
    def __init__(self, prometheus, live: bool):
        self.prometheus = prometheus
        self.data_type = "live" if live else "historical"

        self.time_series_gauge = Gauge(
            "created_time_series_total",
            "Number of time series created since the extractor started running", labelnames=["data_type"]
        )

        self.all_data_points_gauge = Gauge(
            "data_points_posted_total",
            "Number of datapoints posted since the extractor started running", labelnames=["data_type"]
        )

        self.time_series_data_points_gauge = Gauge(
            "data_points_posted_time_series",
            "Number of datapoints posted per time series (based on external ID) since the extractor started running", labelnames=["data_type", "external_id"]
        )
