import os
import pandas
import unittest
from pathlib import Path
from unittest.mock import patch

import datapoints_csv_extractor as extractor


class ExtractorTestCase(unittest.TestCase):
    """Unit tests for `datapoints_csv_extractor.py`"""

    def test_find_files_in_path_historical(self):
        folder_path = "test_files"
        result = extractor.find_files_in_path(Path(folder_path), 0)
        self.assertEqual(len(result), len(os.listdir(folder_path)))

    def test_create_datapoints(self):
        file_path = Path("test_files/TEBIS_FK_1550092560.csv")
        df = pandas.read_csv(file_path, encoding="latin-1", delimiter=";", quotechar='"', skiprows=[1], index_col=0)
        timestamps = [int(o) * 1000 for o in df.index.tolist()]
        values = df.iloc[:,0].values.tolist()

        result = extractor.create_data_points(values, timestamps)
        self.assertEqual(len(result), 60)


if __name__ == "__main__":
    unittest.main()
