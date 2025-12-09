from __future__ import division, print_function
import numpy as np
from pathlib import Path
import polars as pd
from datetime import datetime
import os
import concurrent.futures
import logging



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

    def process_asc_file(self, file_name, locations):
        """Process a single ASC file and extract data for all locations.
        
        Args:
            file_name (str): Name of the ASC file.
            locations (list): List of locations.
            
        Returns:
            list: A list of dictionaries containing extracted data for each location, 
                  or None if processing fails.
                  Format: [{'zone_id': id, 'date': datetime, 'value': float}, ...]
        """
        if not file_name.endswith('.asc'):
            return None

        file_path = Path(self.config.ASC_TOP_FOLDER, file_name)
        results = []

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

                cur_croppedrain = cur_rawgrid[start_row:end_row, start_col:end_col]
                
                if cur_croppedrain.size > 2:
                    val = cur_croppedrain.flatten()[2] / 32
                else:
                    # Handle edge case
                    # print(f"Warning: Crop too small for {zone_id} in {file_name}")
                    val = 0.0

                results.append({
                    'zone_id': zone_id,
                    'date': parsed_date,
                    'value': val
                })

            if self.config.delete_asc_after_processing:
                os.remove(file_path)

            return results
        

        except Exception as e:
            print(f"Error processing file {file_name}: {e}")
            return None

    def extract_data_for_all_locations(self, locations):
        """Extract cropped rain data for all locations by iterating over ASC files concurrently.

        Args:
            locations (list): List of location data [zone_id, easting, northing, zone]
        """
        # Initialize data structure to hold results: {zone_id: {'dates': [], 'values': []}}
        results = {loc[0]: {'dates': [], 'values': []} for loc in locations}

        # Get list of ASC files
        asc_files = sorted(os.listdir(Path(self.config.ASC_TOP_FOLDER)))
        total_files = len(asc_files)
        print(f"Processing {total_files} ASC files concurrently...")

        # Use ThreadPoolExecutor for concurrent processing
        # Since we are using Python 3.14t (free-threaded), this should scale well even for CPU work
        # mixed with I/O.
        with concurrent.futures.ThreadPoolExecutor() as executor:
            # Submit all tasks
            future_to_file = {
                executor.submit(self.process_asc_file, file_name, locations): file_name 
                for file_name in asc_files
            }
            
            completed_count = 0
            try:
                for future in concurrent.futures.as_completed(future_to_file):
                    file_results = future.result()
                    if file_results:
                        for res in file_results:
                            zone_id = res['zone_id']
                            results[zone_id]['dates'].append(res['date'])
                            results[zone_id]['values'].append(res['value'])
                    
                    completed_count += 1
                    if completed_count % 100 == 0:
                        print(f"Processed {completed_count}/{total_files} files")
            except KeyboardInterrupt:
                print("KeyboardInterrupt received. Cancelling pending tasks...")
                executor.shutdown(wait=False, cancel_futures=True)
                raise

    # def write_results_to_csv(self, results, locations):
    #     """Write extracted data to CSV files for each location.

    #     Args:
    #         results (dict): Aggregated results {zone_id: {'dates': [], 'values': []}}
    #         locations (list): List of location data
    #     """
    #     for location in locations:
    #         grid_square = location[0]
    #         zone = location[3]
    #         data = results[grid_square]
            
    #         if not data['dates']:
    #             print(f"No data found for {grid_square}")
    #             continue

    #         df = pd.DataFrame({"datetime": data['dates'], grid_square: data['values']})

    #         # Sort the dataframe into date order
    #         sorted_df = df.sort("datetime")
            
    #         # Format datetime column
    #         sorted_df = sorted_df.with_columns(
    #             pd.col("datetime").dt.strftime("%Y-%m-%d %H:%M:%S")
    #         )

    #         output_path = Path(self.config.CSV_TOP_FOLDER) / f"{zone}_timeseries_data.csv"
    #         sorted_df.write_csv(
    #             output_path,
    #             float_precision=4
    #         )
    #     logging.info("All CSV files written.")

    def write_results_to_csv(self, results, locations):
        """Write extracted data to CSV files for each zone.

        Args:
            results (dict): Aggregated results {zone_id: {'dates': [], 'values': []}}
            locations (list): List of location data [zone_id, easting, northing, zone]
        """
        # Map zone_id -> zone
        zone_map = {loc[0]: loc[3] for loc in locations}

        # Group results by zone and collect all unique dates
        zone_data = {}
        for loc in locations:
            zone_id = loc[0]
            zone_name = loc[3]

            if zone_name not in zone_data:
                zone_data[zone_name] = {'dates': [], 'values': {}}

            zone_data[zone_name]['values'][zone_id] = results[zone_id]['values']
            zone_data[zone_name]['dates'].extend(results[zone_id]['dates'])

        # Get unique sorted dates across all zones
        for zone_name, data in zone_data.items():
            data['dates'] = sorted(set(data['dates']))

        # Now write one CSV per zone with aligned timestamps
        for zone_name, data in zone_data.items():
            dates = data['dates']
            values_dict = data['values']

            # Create aligned DataFrame
            df_dict = {"datetime": dates}
            for grid_square, values in values_dict.items():
                # Align values to the common dates
                aligned_values = []
                value_iter = iter(values)
                date_iter = iter(dates)

                current_date = next(date_iter, None)
                current_value = next(value_iter, None)

                for expected_date in dates:
                    if current_date == expected_date:
                        aligned_values.append(current_value)
                        try:
                            current_date = next(date_iter)
                            current_value = next(value_iter)
                        except StopIteration:
                            current_date = None
                            current_value = None
                    else:
                        aligned_values.append(None)  # Missing value

                df_dict[grid_square] = aligned_values

            df = pd.DataFrame(df_dict)

            # Sort by datetime (already sorted)
            sorted_df = df.sort("datetime")

            # Format datetime column
            sorted_df = sorted_df.with_columns(
                pd.col("datetime").dt.strftime("%Y-%m-%d %H:%M:%S")
            )

            output_path = Path(self.config.COMBINED_FOLDER) / f"{zone_name}_timeseries_data.csv"
            sorted_df.write_csv(output_path, float_precision=4)

        logging.info("All CSV files written.")