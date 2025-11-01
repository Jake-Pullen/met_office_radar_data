from nimrod_3 import Nimrod
import os
from pathlib import Path
import re
import logging # TODO: Add logging

"""
import nimrod  
a = nimrod.Nimrod(open(  
    '200802252000_nimrod_ng_radar_rainrate_composite_1km_merged_UK_zip'))  
a.query()  
a.extract_asc(open('full_raster.asc', 'w'))  
a.apply_bbox(279906, 285444, 283130, 290440)  
a.query()  
a.extract_asc(open('clipped_raster.asc', 'w'))  
"""

BOUNDING_BOX_INFO = {
    "BRISCS": (607000, 608000, 217000, 218000),
    "WINTSC": (499000, 500000, 416000, 417000),
}
in_top_folder = "./dat_files"
out_top_folder = "./asc_files"


def get_datetime(file_name: str) -> str:
    # Pattern to match YYYYMMDDHHMM format
    pattern = r"(\d{8})(\d{4})"
    match = re.search(pattern, file_name)
    if match:
        date_part = match.group(1)  # YYYYMMDD
        time_part = match.group(2)  # HHMM
        return f"{date_part}{time_part}"
    else:
        return "date_not_found"


# read all file names in the folder
area_folders = os.listdir(in_top_folder)

for area in area_folders:
    bounding_box = BOUNDING_BOX_INFO.get(area, (0, 0, 0, 0))
    print(area, bounding_box)
    xmin, xmax, ymin, ymax = bounding_box
    os.makedirs(Path(out_top_folder, area), exist_ok=True)
    for in_file in os.listdir(Path(in_top_folder, area)):
        timestamp = get_datetime(in_file)
        out_file_name = f"{timestamp}_{area}.asc"
        out_file_path = Path(out_top_folder, area, out_file_name)
        in_file_full = Path(in_top_folder, area, in_file)
        #print(in_file_full)
        try:
            image = Nimrod(open(in_file_full, 'rb'))
            image.apply_bbox(xmin, xmax, ymin, ymax)
            # image.query() # prints out file_details
            with open(out_file_path, 'w') as outfile:
                image.extract_asc(outfile)
        except Nimrod.HeaderReadError as e:
            print(f'Failed to read file {in_file_full}, is it corrupt?')
            print(e)
            continue
        except Nimrod.PayloadReadError as e:
            print(f'Failed to load the raster data in {in_file_full}')
            print(e)
            continue
        except Nimrod.BboxRangeError as e:
            print(f'Bounding Box out of range. Given bounding box: {bounding_box}')
            print(e)
            # Skips the whole area as bounding box will be out of bounds for all files
            break 

