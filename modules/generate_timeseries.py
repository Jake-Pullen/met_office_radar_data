from __future__ import division, print_function
import numpy as np
import glob
import pandas as pd
from datetime import datetime


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
        cellres_basin = basin_header[4]

        xp = x0_basin - x0_radar
        yp = y0_basin - y0_radar

        xpp = ncols_basin * cellres_basin
        ypp = nrows_basin * cellres_basin

        start_col = np.floor(xp / cellres_radar)
        end_col = np.ceil((xpp + xp) / cellres_radar)

        start_row = np.floor(nrows_radar - ((yp + ypp) / cellres_radar))
        end_row = np.ceil(nrows_radar - (yp / cellres_radar))

        #print(start_col, start_row, end_col, end_row)
        return int(start_col), int(start_row), int(end_col), int(end_row)


    def extract_cropped_rain_data(self, location):
        """Extract cropped rain data and create rainfall timeseries

        Returns:
            None
        """
        rainfile = []
        datetime_list = []

        for f in glob.iglob(f'{self.config.ASC_TOP_FOLDER}/*.asc'):
            # print(f)
            radar_header = self._read_ascii_header(f)
            start_col, start_row, end_col, end_row = self._calculate_crop_coords(
                location, radar_header
            )

            start_col = int(round(start_col))
            start_row = int(round(start_row))
            end_col = int(round(end_col))
            end_row = int(round(end_row))

            cur_rawgrid = np.genfromtxt(
                f, skip_header=6, filling_values=0.0, loose=True, invalid_raise=False
            )

            cur_croppedrain = cur_rawgrid[start_row:end_row, start_col:end_col]
            # Flatten the cropped rain data into a 1D array
            cur_rainrow = cur_croppedrain.flatten()
            rainfile.append(cur_rainrow[2]/32)

            # Extract datetime from filename
            filename = f.split("/")[-1]  # Get just the filename
            date_str = filename[:8]  # YYYYMMDD
            time_str = filename[8:12]  # HHMM

            # Parse datetime
            parsed_date = datetime.strptime(f"{date_str}{time_str}", "%Y%m%d%H%M")
            datetime_list.append(parsed_date)

        rainfile_arr = np.vstack(rainfile)

        # Create DataFrame with datetime index
        df = pd.DataFrame(rainfile_arr, index=datetime_list)
        # sort the dataframe into date order 
        sorted_df = df.sort_index()
        # add headers 
        header_row = [location[1]]
        file_name = f"csv_files/{location[0]}_timeseries_data.csv"
        sorted_df.to_csv(file_name, sep=",", float_format="%1.4f", header=header_row, index_label='datetime')


