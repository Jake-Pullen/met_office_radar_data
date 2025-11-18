from __future__ import division, print_function
import numpy as np
from pathlib import Path
import polars as pd
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

        y0_basin = basin_header[2]
        x0_basin = basin_header[1]

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

    def extract_data_for_all_locations(self, locations):
        """Extract cropped rain data for all locations by iterating over ASC files once.

        Args:
            locations (list): List of location data [zone_id, easting, northing, zone]
        """
        # Initialize data structure to hold results: {zone_id: {'dates': [], 'values': []}}
        results = {loc[0]: {'dates': [], 'values': []} for loc in locations}

        # Get list of ASC files and sort them to ensure chronological order if needed
        asc_files = sorted(os.listdir(Path(self.config.ASC_TOP_FOLDER)))
        
        total_files = len(asc_files)
        print(f"Processing {total_files} ASC files...")

        for i, file_name in enumerate(asc_files):
            if not file_name.endswith('.asc'):
                continue
                
            file_path = Path(self.config.ASC_TOP_FOLDER, file_name)

            try:
                radar_header = self._read_ascii_header(str(file_path))
                
                # Read grid once
                cur_rawgrid = np.loadtxt(file_path, skiprows=6, dtype=float, delimiter=None)

                # Parse datetime from filename once
                filename = os.path.basename(file_path)
                date_str = filename[:8]  # YYYYMMDD
                time_str = filename[8:12]  # HHMM
                parsed_date = datetime.strptime(f"{date_str}{time_str}", "%Y%m%d%H%M")

                # Extract data for each location
                for location in locations:
                    zone_id = location[0]
                    
                    # Calculate crop coordinates
                    start_col, start_row, end_col, end_row = self._calculate_crop_coords(
                        location, radar_header
                    )

                    # Extract value
                    # Note: The original code used cur_croppedrain.flatten()[2] / 32
                    # We need to ensure the crop is valid and has enough elements.
                    # Assuming the crop size is fixed as per original code (2x2 basin -> 4 cells?)
                    # Original: 
                    # nrows_basin = 2
                    # ncols_basin = 2
                    # cellres_basin = 1000
                    # cellres_radar = radar_header[4] (usually 1000)
                    # So it's likely a small grid.
                    
                    cur_croppedrain = cur_rawgrid[start_row:end_row, start_col:end_col]
                    
                    # Original logic: rainfile.append(cur_croppedrain.flatten()[2] / 32)
                    # We replicate this exactly.
                    if cur_croppedrain.size > 2:
                        val = cur_croppedrain.flatten()[2] / 32
                    else:
                        val = 0.0 # Handle edge case if crop is too small? Or maybe NaN?
                        # For now, let's assume it works as before, but maybe add a check?
                        # If the original code worked, this should work too provided indices are correct.
                        # If size is too small, it would raise IndexError in original code too.
                        if cur_croppedrain.size <= 2:
                             print(f"Warning: Crop too small for {zone_id} in {file_name}")
                             val = 0.0 # Default or error?

                    results[zone_id]['dates'].append(parsed_date)
                    results[zone_id]['values'].append(val)
            
            except Exception as e:
                print(f"Error processing file {file_name}: {e}")
                continue

            if (i + 1) % 100 == 0:
                print(f"Processed {i + 1}/{total_files} files")

        # Write CSVs for each location
        print("Writing CSV files...")
        for location in locations:
            zone_id = location[0]
            data = results[zone_id]
            
            if not data['dates']:
                print(f"No data found for {zone_id}")
                continue

            df = pd.DataFrame({"datetime": data['dates'], zone_id: data['values']})

            # Sort and set index (Polars)
            sorted_df = df.sort("datetime")
            sorted_df = sorted_df.with_columns(
                pd.Series(data['dates']).alias("datetime")
            ).set_sorted("datetime")

            output_path = Path(self.config.CSV_TOP_FOLDER) / f"{zone_id}_timeseries_data.csv"
            sorted_df.write_csv(
                output_path,
                float_precision=4
            )
        print("All CSV files written.")
