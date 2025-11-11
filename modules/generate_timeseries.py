from __future__ import division, print_function
import numpy as np
from pathlib import Path
import pandas as pd
from datetime import datetime
import os


class GenerateTimeseries:
    def __init__(self, config):
        self.config = config

    def _read_ascii_header(self, ascii_raster_file: str) -> list:
        """Reads header information from an ASCII DEM

        Args:
            ascii_raster_file (str): Path to the ASCII raster file

        Returns:
            list: Header data as a list of floats
        """
        with open(ascii_raster_file) as f:
            header_data = [float(f.__next__().split()[1]) for x in range(6)]
        return header_data

    def _calculate_crop_coords(self, basin_header: list, radar_header: list) -> tuple:
        """Calculate crop coordinates based on header data

        Args:
            basin_header (list): Basin header data
            radar_header (list): Radar header data

        Returns:
            tuple: (start_col, start_row, end_col, end_row) as integers
        """
        y0_radar = radar_header[3]
        x0_radar = radar_header[2]

        y0_basin = basin_header[3]
        x0_basin = basin_header[2]

        nrows_radar = radar_header[1]

        nrows_basin = 2  # hardcoded, likely to change?
        ncols_basin = 2  # hardcoded, likely to change?

        cellres_radar = radar_header[4]
        cellres_basin = 1000  # 1km

        xp = x0_basin - x0_radar
        yp = y0_basin - y0_radar

        xpp = ncols_basin * cellres_basin
        ypp = nrows_basin * cellres_basin

        start_col = np.floor(xp / cellres_radar)
        end_col = np.ceil((xpp + xp) / cellres_radar)

        start_row = np.floor(nrows_radar - ((yp + ypp) / cellres_radar))
        end_row = np.ceil(nrows_radar - (yp / cellres_radar))

        return int(start_col), int(start_row), int(end_col), int(end_row)

    def extract_cropped_rain_data(self, location):
        """Extract cropped rain data and create rainfall timeseries

        Returns:
            None
        """
        rainfile = []
        datetime_list = []

        for file_name in os.listdir(Path(self.config.ASC_TOP_FOLDER)):
            file_path = Path(self.config.ASC_TOP_FOLDER, file_name)

            radar_header = self._read_ascii_header(str(file_path))

            # Calculate crop coordinates
            start_col, start_row, end_col, end_row = self._calculate_crop_coords(
                location, radar_header
            )

            cur_rawgrid = np.loadtxt(file_path, skiprows=6, dtype=float, delimiter=None)

            cur_croppedrain = cur_rawgrid[start_row:end_row, start_col:end_col]

            rainfile.append(cur_croppedrain.flatten()[2] / 32)

            # Extract datetime from filename
            filename = os.path.basename(file_path)  # Get just the filename
            date_str = filename[:8]  # YYYYMMDD
            time_str = filename[8:12]  # HHMM

            # Parse datetime
            parsed_date = datetime.strptime(f"{date_str}{time_str}", "%Y%m%d%H%M")
            datetime_list.append(parsed_date)

        # Create DataFrame with datetime index
        df = pd.DataFrame({"rainfall": rainfile}, index=datetime_list)

        # Sort the dataframe into date order
        sorted_df = df.sort_index()

        sorted_df.to_csv(
            f"csv_files/{location[0]}_timeseries_data.csv",
            sep=",",
            float_format="%1.4f",
            header=[location[1]],
            index_label="datetime",
        )
