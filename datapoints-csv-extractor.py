import os
import time
import glob

import pandas as pd
from cognite import CogniteClient
from cognite.client.stable.datapoints import TimeseriesWithDatapoints, Datapoint


API_KEY = os.environ.get('COGNITE_EXTRACTOR_API_KEY')
if not API_KEY:
    print('COME ON JAN&SAM, YOU FORGOT THE API KEY!')
    sys.exit(2)

# Extract datapoints stored in CSV format
def process_datapoints():
    client = CogniteClient(api_key=API_KEY)

    # List of time series being processed
    current_time_series = []

    # Maximum number of time series batched at once
    batch_max = 1000

    # Path to folder of CSV files
    folder_path = "../TebisSampleData/"

    # Loop through all CSV folders, posting all datapoints in the file
    for filename in glob.glob(folder_path + "/*.csv"):
        # Read CSV, removing first row containing units of data
        df = pd.read_csv(os.path.join(folder_path, filename), encoding='latin-1', delimiter=';', quotechar='"', skiprows=[1], index_col=0)

        timestamps = df.index.tolist()

        for col in df:
            # Post datapoints if length list of time series being processed is greater than the maximum, then empty list
            if len(current_time_series) >= batch_max:
                client.datapoints.post_multi_time_series_datapoints(current_time_series)
                current_time_series = []

            values = df[col].tolist()
            datapoints = []
            time_series = str(col.rpartition(':')[2].strip())

            for i in range(len(values)):
                if pd.notnull(values[i]):
                    # Convert from European data format
                    current_value = float(values[i].replace(',', '.'))
                    datapoints.append(Datapoint(timestamp=int(timestamps[i])*1000, value=current_value))
            if datapoints:
                current_time_series.append(TimeseriesWithDatapoints(name=time_series, datapoints=datapoints))

        # If list of datapoints being processed is not empty, post leftover datapoints
        if current_time_series:
            client.datapoints.post_multi_time_series_datapoints(current_time_series)
            current_time_series = []

        print("Completed 1 file")

        # Delay processing between files by 1 second
        time.sleep(1)

if __name__ == '__main__':
    process_datapoints()
