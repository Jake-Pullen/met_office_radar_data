import logging
import time
import os
from pathlib import Path

from config import Config
from modules import BatchNimrod, GenerateTimeseries

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

if __name__ == "__main__":
    os.makedirs(Path(Config.ASC_TOP_FOLDER), exist_ok=True)
    os.makedirs(Path(Config.CSV_TOP_FOLDER), exist_ok=True)
    dat_file_count = [f for f in os.listdir(Path(Config.DAT_TOP_FOLDER))]
    asc_file_count = [f for f in os.listdir(Path(Config.ASC_TOP_FOLDER))]

    locations = [
        # loc name, loc id, x loc,   y loc,  resolution
        ["BRICSC", "TM0816", 608500, 216500, 1000],  
        ["HEACSC", "TF6842", 568500, 342500, 1000], 
    ]

    batch = BatchNimrod(Config)
    timeseries = GenerateTimeseries(Config)

    start = time.time()
    logging.info("Starting to process DAT to ASC")
    if dat_file_count != asc_file_count:
        batch.process_nimrod_files()
        batch_checkpoint = time.time()
        elapsed_time = batch_checkpoint - start
        logging.info(f"DAT to ASC completed in {elapsed_time:.2f} seconds")
    else:
        logging.info("No need to process DAT files, skipping...")
        time.sleep(1)

    for place in locations:
        logging.info(f'{place[0]} started generating timeseries data.')
        timeseries.extract_cropped_rain_data(place)
        place_checkpoint = time.time()
        since_asc_create = place_checkpoint - batch_checkpoint
        elapsed_time = place_checkpoint - start
        logging.info(f"{place[0]} completed in {since_asc_create:.2f} seconds")
        logging.info(f'total time so far {elapsed_time:.2f} seconds')
    
    logging.info(f'All Complete')