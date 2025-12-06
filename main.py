import logging
import time
import os
import csv
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
    logging.info("Starting to process DAT to ASC")

    batch.process_nimrod_files()
    batch_checkpoint = time.time()
    elapsed_time = batch_checkpoint - start
    logging.info(f"DAT to ASC completed in {elapsed_time:.2f} seconds")

    logging.info("Starting generating timeseries data for all locations.")
    place_start = time.time()
    timeseries.extract_data_for_all_locations(locations)
    place_end = time.time()
    place_create_time = place_end - place_start
    elapsed_time = place_end - start
    logging.info(f"Timeseries generation completed in {place_create_time:.2f} seconds")
    logging.info(f"Total time so far {elapsed_time:.2f} seconds")

    logging.info("combining CSVs into groups")
    combiner.combine_csv_files()
    logging.info("CSVs combined!")
    end = time.time()
    elapsed_time = end - start

    logging.info(f"All Complete total time {elapsed_time:.2f} seconds")
