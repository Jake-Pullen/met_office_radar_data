# UK Met Office Rain Radar NIMROD Data Processor

This project provides tools for processing UK Met Office Rain Radar NIMROD image files. It allows extraction of raster data from NIMROD .dat format files and conversion to ESRI ASCII (.asc) format with optional bounding box clipping.

## Overview

The project consists of a main pipeline workflow that processes multiple modules in sequence:
- `main.py`: Main pipeline orchestrator that calls on the modules as needed
- `batch_nimrod.py`: Module for batch processing multiple NIMROD files with configurable bounding boxes
- `generate_timeseries.py`: Module for extracting cropped rain data and creating rainfall timeseries
- `combine_timeseries.py`: Module for combining grouped timeseries CSVs into consolidated datasets

## Features

### main.py
- Orchestrates the entire workflow pipeline
- Processes DAT files to ASC format
- Generates timeseries data for specified locations
- Combines grouped CSV files into consolidated datasets

### batch_nimrod.py
- Process multiple NIMROD dat files
- Automatically extract datetime from file data
- Export clipped raster data to ASC format

### generate_timeseries.py
- Extract cropped rain data based on specified locations
- Create rainfall timeseries CSVs for each location
- Parse datetime from filename and create proper datetime index

### combine_timeseries.py
- Combine multiple timeseries CSV files into grouped datasets
- Group locations by specified output groups
- Create consolidated CSV files for each group

## Usage

It is recommended to use UV for environment and package handling.
[Link to uv install](https://docs.astral.sh/uv/getting-started/installation/)


### Main Pipeline (main.py)
```bash
uv run main.py
```

The main pipeline will:
1. Process DAT files to ASC format if needed
2. Generate timeseries data for specified locations
3. Combine grouped CSV files into consolidated datasets

## Configuration

The `config.py` file defines folder paths:
- DAT_TOP_FOLDER: "./dat_files"
- ASC_TOP_FOLDER: "./asc_files"
- CSV_TOP_FOLDER: "./csv_files"
- COMBINED_FOLDER: "./combined_files"

The `main.py` script defines locations and their properties:
- Location name (e.g., "BRICSC")
- Location ID (e.g., "TM0816")
- X coordinate (e.g., 608500)
- Y coordinate (e.g., 216500)
- Output group (e.g., 1)

## Directory Structure

```
dat_files/
└──*.dat files

asc_files/
└──*.dat files

csv_files/
├── TQ1234_timeseries_data.csv
├── ...
└── TQ5678_timeseries_data.csv
combined_files/
├── zone_1_timeseries_data.csv
├── ...
└── zone_50_timeseries_data.csv
```

## Requirements

- Python 3.12+
- [UV Installed](https://docs.astral.sh/uv/getting-started/installation/)

## Acknowledgments 

[Richard Thomas - Original Nimrod dat to asc file conversion](https://github.com/richard-thomas/MetOffice_NIMROD)
[Declan Valters - building the timeseries from the asc files](https://github.com/dvalters/NIMROD-toolbox)

## Version update 2025

Update by Jake Pullen, for the use of Anglian Water.
Added the batch_nimrod module to convert large amounts of files
Cleaned up the original codes and added docstrings & typehints
Added main pipeline workflow that calls on the modules as needed to take the dat files and create grouped timeseries data CSVs
