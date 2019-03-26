# coding: utf-8
"""
A module for testing the extractor.
"""
import os
import unittest
from pathlib import Path

import pandas

from csv_extractor import create_data_points, find_files_in_path

TEST_FOLDER = Path(__file__).parent / "test_files"


class ExtractorTestCase(unittest.TestCase):
    """Unit tests for `main.py`"""

    def test_find_files_in_path_historical(self):
        folder_path = TEST_FOLDER
        result = find_files_in_path(Path(folder_path), 0)
        self.assertEqual(len(result), len(os.listdir(folder_path)))

    def test_create_datapoints(self):
        file_path = TEST_FOLDER / "TEBIS_FK_1550092560.csv"
        df = pandas.read_csv(file_path, encoding="latin-1", delimiter=";", quotechar='"', skiprows=[1], index_col=0)
        timestamps = [int(o) * 1000 for o in df.index.tolist()]
        values = df.iloc[:, 0].values.tolist()

        result = create_data_points(values, timestamps)
        self.assertEqual(len(result), 60)


if __name__ == "__main__":
    unittest.main()
