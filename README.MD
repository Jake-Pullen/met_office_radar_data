# UK Met Office Rain Radar NIMROD Data Processor

This project provides tools for processing UK Met Office Rain Radar NIMROD image files. It allows extraction of raster data from NIMROD .dat format files and conversion to ESRI ASCII (.asc) format. It also allows the creation of timeseries data from the ASC files, formatted for Infoworks ICM. 

## Overview

The project consists of a main pipeline workflow that processes multiple modules in sequence:

- `main.py`: Main pipeline orchestrator that calls on the modules as needed
- `batch_nimrod.py`: Module for batch processing multiple NIMROD files with configurable bounding boxes
- `generate_timeseries.py`: Module for extracting cropped rain data and creating rainfall timeseries
- `extract.py`: Module for extracting the dat files from the .gz.tar files that are downloaded from source

## Features

### main.py

- **Startup Safety Check**: Scans the `COMBINED_FOLDER` at startup and warns the user if existing files are found, offering a chance to abort to prevent accidental data mixing.
- **Batch Processing**: Processes input tar files in configurable batches to manage resource usage.
- **End-to-End Processing**: Extracts GZ files, processes DAT/ASC, and appends to CSV in a single thread per file.
- **Concurrency**: Uses multi-threading to process individual GZ files within a batch concurrently.
- **Cumulative Data**: Automatically appends new query results to the existing CSV files in `COMBINED_FOLDER` for each batch, ensuring no data is lost and columns are correctly aligned.
- **Dynamic ETA**: Provides a real-time estimate of completion time.

### extract.py

- Converts all .gz.tar files first to 288 (1 day) of .gz files
- Converts all .gz files to .dat files ready for processing.

### batch_nimrod.py

- Process multiple NIMROD dat files
- Automatically extract datetime from file data
- Export clipped raster data to ASC format

### generate_timeseries.py

- Extract cropped rain data based on specified locations
- Create rainfall timeseries CSVs for each location
- Parse datetime from filename and create proper datetime index
- Group locations by specified output groups
- Create consolidated CSV files for each group

## Requirements

This is a multi-threaded application and requires Python 3.14t (free-threaded) to run correctly and efficiently. Please ensure you are using the free-threaded build of Python 3.14.

It is recommended to use UV for environment and package handling.
[Link to uv install](https://docs.astral.sh/uv/getting-started/installation/)

## Usage

1. Ensure all required packages are installed `uv sync`
1. Adjust the config.py file to match your needs.
1. Ensure your .gz.tar files are in the TAR_TOP_FOLDER (as per config location)
1. Ensure your zone csv files are in the ZONE_FOLDER (as per config location)
1. RunMain Pipeline `uv run main.py` Note that you will have to set your environment variable `PYTHON_GIL=0` first
1. find the output in the COMBINED_FOLDER (as per config location)

The main pipeline will:

1. Uncompress the .gz.tar files ready for processing
1. Process DAT files to ASC format
1. Generate timeseries data for specified locations
1. Combine grouped locations into consolidated datasets

## Configuration

The `config.py` file defines folder paths and file deletion options:

- TAR_TOP_FOLDER = "./tar_files"
- GZ_TOP_FOLDER = "./gz_files"
- DAT_TOP_FOLDER = "./dat_files"
- ASC_TOP_FOLDER = "./asc_files"
- COMBINED_FOLDER = "./combined_files"
- ZONE_FOLDER = "./zone_inputs"
- BATCH_SIZE = 5 (Number of tar files to process per batch)

Example of how the zone csv files should look:

```csv
1K Grid, easting, northing, zone_number
TM0816, 608500, 216500, 1
TF6842, 568500, 342500, 1
```

## Acknowledgments 

Thank you to the following projects for their inspiration and code:  

- [Richard Thomas - Original Nimrod dat to asc file conversion](https://github.com/richard-thomas/MetOffice_NIMROD)
- [Declan Valters - building the timeseries from the asc files](https://github.com/dvalters/NIMROD-toolbox)
