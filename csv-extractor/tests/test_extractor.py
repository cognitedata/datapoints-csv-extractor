# coding: utf-8
"""
A module for testing the extractor.
"""
import os
from pathlib import Path

import pandas

from csv_extractor import create_data_points, find_historical_files_in_path


class TestExtractor:
    folder_path = Path(__file__).parent / "test_files"

    def test_find_files_in_path_historical(self):
        result = find_historical_files_in_path(Path(self.folder_path), None, None)
        assert len(result), len(os.listdir(self.folder_path))

    def test_create_datapoints(self):
        file_path = self.folder_path / "TEBIS_FK_1550092560.csv"
        df = pandas.read_csv(file_path, encoding="latin-1", delimiter=";", quotechar='"', skiprows=[1], index_col=0)
        timestamps = [int(o) * 1000 for o in df.index.tolist()]
        values = df.iloc[:, 0].values.tolist()

        result = create_data_points(values, timestamps)
        assert len(result), 60
