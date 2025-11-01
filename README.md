# UK Met Office Rain Radar NIMROD Data Processor

This project provides tools for processing UK Met Office Rain Radar NIMROD image files. It allows extraction of raster data from NIMROD format files and conversion to ESRI ASCII (.asc) format with optional bounding box clipping.

## Overview

The project consists of two main Python modules:
- `nimrod.py`: Core library for parsing NIMROD files, extracting metadata, and converting to ASCII format
- `batch_nimrod.py`: Script for batch processing multiple NIMROD files with configurable bounding boxes

## Features

### nimrod.py
- Parse NIMROD format files (v1.7 and v2.6-4)
- Extract header information and metadata
- Convert raster data to ESRI ASCII (.asc) format
- Apply bounding box clipping to extract specific regions
- Support for command-line usage or import as module

### batch_nimrod.py
- Process multiple NIMROD files in batches
- Apply configurable bounding boxes per area
- Automatically extract datetime from filenames
- Export clipped raster data to ASC format

## Usage

### Command Line (nimrod_3.py)
```bash
python nimrod.py [-h] [-q] [-x] [-bbox XMIN XMAX YMIN YMAX] [infile] [outfile]
```

Options:
- `-h, --help`: Show help message
- `-q, --query`: Display metadata
- `-x, --extract`: Extract raster file in ASC format
- `-bbox XMIN XMAX YMIN YMAX`: Bounding box to clip raster data to

### Python Example Module Usage (nimrod.py)
```python
from nimrod import Nimrod
# Open the .dat or nimrod compliant file
a = Nimrod(open('filename.dat'))
# Show the information about the file
a.query()
# output the .asc file
a.extract_asc(open('output.asc', 'w'))
# shrink the file down to a box area
a.apply_bbox(279906, 285444, 283130, 290440)
# show the shrunken down information about the file 
a.query()
# output the shrunken down .asc file
a.extract_asc(open('clipped_output.asc', 'w'))
```

### Batch Processing (batch_nimrod.py)
It is recommended to use UV for environment and package handling.
[Link to uv install](https://docs.astral.sh/uv/getting-started/installation/)

```bash
uv sync 
uv run batch_nimrod.py
```

## Configuration

The `config.yaml` file defines bounding box information for different areas. Default configuration includes:
- BRISCS: (607000, 608000, 217000, 218000)
- WINTSC: (499000, 500000, 416000, 417000)

## Directory Structure

Inside the dat_files folder, each site short code should be its own folder with the .dat files inside of them.  
The site short code folder name should match EXACTLY to the config site short code  
Each dat file should have a datetime in its name in the format of yyyymmddhhmm (e.g: 202405260905 )  

```
dat_files/
├── BRISCS/
│   └── *.dat files
└── WINTSC/
    └── *.dat files
asc_files/
├── BRISCS/
│   └── YYYYMMDDHHMM_BRISCS.asc files
└── WINTSC/
    └── YYYYMMDDHHMM_WINTSC.asc files
```

## Requirements

- Python 3.12+
- [UV Installed](https://docs.astral.sh/uv/getting-started/installation/)

## License

Copyright (c) 2015 [Richard Thomas](https://github.com/richard-thomas/MetOffice_NIMROD)

This program is free software: you can redistribute it and/or modify it under the terms of the Artistic License 2.0 as published by the Open Source Initiative.

This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.

## Version update 2025-10-31 👻

Update by Jake Pullen, for the use of Anglian Water.
Added the batch_nimrod module to convert large amounts of files
Cleaned up the original code and added docstrings & typehints
