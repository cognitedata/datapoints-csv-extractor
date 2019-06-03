# datapoints-csv-extractor
An extractor utilized to extract datapoints stored in CSV format

## Overview of extractor
- The extractor is composed of a Python script (Python v3.7.2) that extracts datapoints stored in CSV format (the specific format of the file is discussed below), formats them, and pushes them to the CDF
- This script can be run for either `live` or `historical` CSVs in a given folder
    - Live: The script will process the 20 most recent files in the folder, and then proceed to check for any new files added to the folder every 8 seconds indefinitely
    - Historical: The script will process data from all files in the folder, newest first, and then stop
- The script either removes the files from the source after successfully processing the data, or moves them to subfolder

## File format
The data that the script is capable of processing is in the following format:
- CSVs encoded in `latin-1` that have a delimiter of `;` and quote character of `"`
- First row includes names of the time series (one per column) stored in the following format: `external_id: time_series_name`
    - external_id: the unique ID that the client attaches to a given time series. It is assumed that the external_id of the time series never changes
    - time_series_name: the name of the time series, and method of how the CDF maps datapoints of a time series to the metadata of that time series. It is assumed that the time_series_name may change on the client's end
- Second row includes the unit of the time series. This is already stored in the time series metadata, and is thus ignored.
- Following rows are stored in the following manner
    - First column: timestamps of the datapoints in Epoch time
    - Following columns: values of a datapoint at a given time for a given time series

## Deploying the extractor
At the very minimum, the script needs to have an input folder that holds the CSV files that the user wants processed.

By default, the script runs assuming that the user desires to process *historical* data. This means that the script will not continue to check for new files, and instead will stop running after processing files detected when initially run.

If not utilizing the `--api-key` command line flag, then it is necessary to configure an environment variable called `COGNITE_EXTRACTOR_API_KEY` or `COGNITE_API_KEY` that stores the API key for CDF.

On an iOS device:
```
python datapoints-csv-extractor.py --input <INPUT_FOLDER>
```

On a Windows device:
```
<PYTHON PATH>\python.exe datapoints-csv-extractor.py --input <INPUT_FOLDER>
```
- Where <PYTHON PATH> is the path to the Python interpreter on the device. 

**Command line arguments**

| Long Tag | Short Tag | Required | Parameter Required | Description |
| ----------- | -----------| -----------| ----------- | ----------- |
| --input | -i | TRUE | TRUE | Specify the folder that holds the CSV files that the user wants processed. |
| --historical |  | | FALSE | Flag to process historical data (redundant). By default, the script will process live data. |
| --live | -l | | FALSE |  Flag required to process live data. If this flag is not used, then the script will process historical data. |
| --api-key | -k | FALSE | TRUE | If this flag is not use, the script will attempt to pull the API key from an environment variable called `COGNITE_EXTRACTOR_API_KEY`|
| --log | -d | FALSE | TRUE |  Specify the folder that log files will be created. |
| --log-level | | FALSE | TRUE |  Which log level should be logged. Default INFO. |
| --move-failed | | FALSE | FALSE |  If this flag is used, the script will move CSV files failed to process into a subfolder called `failed/` |
| --keep-finished | | FALSE | FALSE |  If this flag is used, the script will move finished CSV files  into a subfolder called `finished/` |

## Contributing

Feel free to open [issues](https://github.com/cognitedata/datapoints-csv-extractor/issues) / [pull requests](https://github.com/cognitedata/datapoints-csv-extractor/pulls) on [GitHub](https://github.com/cognitedata/datapoints-csv-extractor).

## Versioning

[Changelog](https://github.com/cognitedata/datapoints-csv-extractor/blob/master/CHANGELOG.md) can be found on GitHub.

[SemVer](https://semver.org/). See [repository tags](https://github.com/cognitedata/datapoints-csv-extractor/tags).

## Tests
Follow command will run unit tests and generate coverage report:
```
pipenv run pytest --cov=csv-extractor
```
