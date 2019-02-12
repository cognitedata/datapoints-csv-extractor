import glob
import os
import re
import time

import pandas as pd
from cognite import CogniteClient
from cognite.client.stable.datapoints import TimeseriesWithDatapoints, Datapoint


API_KEY = os.environ.get('COGNITE_EXTRACTOR_API_KEY')
if not API_KEY:
    print('COME ON JAN&SAM, YOU FORGOT THE API KEY!')
    sys.exit(2)


# Global variable for last timestamp processed
last_processed = 1548975600

# Maximum number of time series batched at once
batch_max = 1000

# Path to folder of CSV files
folder_path = '../TebisSampleData2/'

# File path of files processed. Stores Epoch timestamp
epoch_path = 'epoch_timestamp.txt'

client = CogniteClient(api_key=API_KEY)

# Input: a list of absolute or relative file paths of CSVs containing datapoints to process
def post_datapoints(files):
    # List of time series being processed
    current_time_series = []

    # Loop through all CSV folders, posting all datapoints in the file
    for filename in files:
        # Read CSV, removing first row containing units of data
        df = pd.read_csv(os.path.join(folder_path, filename), encoding='latin-1', delimiter=';', quotechar='"', skiprows=[1], index_col=0)
        indices_list = df.index.tolist()

        for col in df:
            # Post datapoints if list of time series being processed is greater than the maximum, then empty list
            if len(current_time_series) >= batch_max:
                client.datapoints.post_multi_time_series_datapoints(current_time_series)
                current_time_series = []

            values_list = df[col].tolist()
            datapoints_list = []
            time_series = str(col.rpartition(':')[2].strip())

            for i in range(len(values_list)):
                # Format Datapoint and TimeseriesWithDatapoints
                if pd.notnull(values_list[i]):
                    value = float(values_list[i].replace(',', '.'))
                    datapoints_list.append(Datapoint(timestamp=int(indices_list[i])*1000, value=value))
            if datapoints_list:
                current_time_series.append(TimeseriesWithDatapoints(name=time_series, datapoints=datapoints_list))

        # If list of datapoints being processed is not empty, post leftover datapoints
        if current_time_series:
            client.datapoints.post_multi_time_series_datapoints(current_time_series)
            current_time_series = []

        os.remove(filename)
        print('Processed: ' + filename)

    # Update the timestamp compared to if new files are processed
    global last_processed
    if files:
        last_processed = int(re.findall(r'\d+', files[-1])[-1])
        with open(epoch_path, 'a') as f:
            f.write(str(last_processed) + '\n')

    # Delay processing between files by a second
    time.sleep(1)

# Find new files, if existent and post datapoints from that file
def find_new_files():
    # Find all files in filepath and their corresponding timestamps
    all_files = sorted(glob.glob(folder_path + '/*.csv'))
    all_timestamps = [re.findall(r'\d+', x)[-1] for x in all_files]
    all_timestamps = list(map(int, all_timestamps))

    # Find files from a timestamp later than the last processed file
    global last_processed
    with open(epoch_path) as f:
        for line in f:
            pass
        last_line = line
        last_processed = int(last_line)
    new_timestamps = [i for i, x in enumerate(all_timestamps) if x > last_processed]
    new_files = [all_files[i] for i in new_timestamps]

    if new_files:
        post_datapoints(new_files)

    time.sleep(5)

def extract_datapoints():
    initial_files = sorted(glob.glob(folder_path + '/*.csv'))
    post_datapoints(initial_files)
    while True:
        find_new_files()

if __name__ == '__main__':
    extract_datapoints()
