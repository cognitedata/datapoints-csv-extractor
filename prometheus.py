class Prometheus:
    def __init__(self, prometheus):
        self.prometheus = prometheus

        self.data_points_gauge = self.prometheus.get_gauge(
            "number_data_points_posted",
            "Number of datapoints posted per file processed"
        )

        self.created_time_series_gauge = self.prometheus.get_gauge(
            "number_created_time_series",
            "Number of time series created by extractor"
        )
