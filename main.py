import logging
import time
import os
import csv
import concurrent.futures
from pathlib import Path

from config import Config
from modules import BatchNimrod, GenerateTimeseries, CombineTimeseries

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

if __name__ == "__main__":
    os.makedirs(Path(Config.ASC_TOP_FOLDER), exist_ok=True)
    os.makedirs(Path(Config.CSV_TOP_FOLDER), exist_ok=True)
    os.makedirs(Path(Config.COMBINED_FOLDER), exist_ok=True)

    locations = []
    #load zone inputs here
    for file in os.listdir(Path(Config.ZONE_FOLDER)):
        with open(Path(Config.ZONE_FOLDER,file), 'r') as csvfile:
            reader = csv.reader(csvfile)
            header = next(reader)  # Skip header row
            for row in reader:
                # Extract the relevant fields: Ossheet (location ID), Easting, Northing, Zone
                zone_id = row[1]  # Ossheet column
                easting = int(row[2])  # Easting column
                northing = int(row[3])  # Northing column
                zone = int(row[6])  # ZoneID column
                locations.append([zone_id, easting, northing, zone])

    batch = BatchNimrod(Config)
    timeseries = GenerateTimeseries(Config)
    combiner = CombineTimeseries(Config, locations)

    start = time.time()
    logging.info("Starting interleaved processing of DAT files and Timeseries generation")
    
    # Initialize results structure
    results = {loc[0]: {'dates': [], 'values': []} for loc in locations}

    def process_pipeline(dat_file):
        # 1. Process DAT to ASC
        asc_file = batch._process_single_file(dat_file)
        if not asc_file:
            return None
        
        # 2. Extract data from ASC
        file_results = timeseries.process_asc_file(asc_file, locations)
        return file_results

    # Get list of DAT files
    dat_files = [f for f in os.listdir(Path(Config.DAT_TOP_FOLDER)) if not f.startswith('.')]
    total_files = len(dat_files)
    
    logging.info(f"Processing {total_files} files concurrently...")

    with concurrent.futures.ThreadPoolExecutor() as executor:
        future_to_file = {
            executor.submit(process_pipeline, dat_file): dat_file 
            for dat_file in dat_files
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
                if completed_count % 10 == 0:
                    logging.info(f'Processed {completed_count} out of {total_files} files')
        except KeyboardInterrupt:
            logging.warning("KeyboardInterrupt received. Cancelling pending tasks...")
            executor.shutdown(wait=False, cancel_futures=True)
            raise

    elapsed_time = time.time() - start
    logging.info(f"Interleaved processing completed in {elapsed_time:.2f} seconds")

    logging.info("Writing CSV files...")
    timeseries.write_results_to_csv(results, locations)
    
    logging.info("combining CSVs into groups")
    combiner.combine_csv_files()
    logging.info("CSVs combined!")
    end = time.time()
    elapsed_time = end - start

    logging.info(f"All Complete total time {elapsed_time:.2f} seconds")
