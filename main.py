import logging
import time
import os
import csv
import concurrent.futures
from pathlib import Path
import shutil

from config import Config
from modules import BatchNimrod, GenerateTimeseries, Extract

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


def process_pipeline(gz_file_path):
    # 1. Extract GZ to DAT
    gz_path = Path(gz_file_path)
    # The dat file name is derived from the gz file name (removing .gz or .dat.gz)
    # gz files are named like 'NAME.dat.gz' often.
    dat_filename = gz_path.name.replace(".gz", "")
    dat_path = Path(Config.DAT_TOP_FOLDER, dat_filename)

    # Extract
    try:
        extraction.process_single_gz(gz_path, dat_path)
    except Exception as e:
        logging.error(f"Failed to extract {gz_path}: {e}")
        return None

    if not dat_path.exists():
        logging.error(f"DAT file not found after extraction: {dat_path}")
        return None

    # 2. Process DAT to ASC
    # BatchNimrod._process_single_file expects just the filename, not full path
    asc_file = batch._process_single_file(dat_filename)
    if not asc_file:
        # Cleanup failed DAT file if needed (BatchNimrod might have done it or not)
        if Config.delete_dat_after_processing and dat_path.exists():
            try:
                os.remove(dat_path)
            except OSError:
                pass
        return None

    # 3. Extract data from ASC
    file_results = timeseries.process_asc_file(asc_file, locations)

    return file_results


def initialise_folders():
    folder_list = [
        Config.ASC_TOP_FOLDER,
        Config.COMBINED_FOLDER,
        Config.GZ_TOP_FOLDER,
        Config.DAT_TOP_FOLDER,
        Config.TAR_TOP_FOLDER,
    ]
    for path in folder_list:
        Path(path).mkdir(exist_ok=True)


if __name__ == "__main__":
    initialise_folders()

    locations = []
    zones = set()
    # load zone inputs here
    for file in os.listdir(Path(Config.ZONE_FOLDER)):
        with open(Path(Config.ZONE_FOLDER, file), "r") as csvfile:
            reader = csv.reader(csvfile)
            header = next(reader)  # Skip header row
            for row in reader:
                # Extract the relevant fields: 1K Grid, Easting, Northing, Zone
                grid_name = row[0]  # 1k Grid name
                easting = int(row[1])  # Easting column
                northing = int(row[2])  # Northing column
                zone = int(row[3])  # ZoneID column
                locations.append([grid_name, easting, northing, zone])
                zones.add(zone)
    logging.info(f"Count of 1km Grids: {len(locations)}")
    logging.info(f"Count of Zones: {len(zones)}")

    # Check for existing combined files
    existing_combined = os.listdir(Config.COMBINED_FOLDER)
    if existing_combined:
        logging.warning("!" * 80)
        logging.warning(
            f"Found {len(existing_combined)} files in {Config.COMBINED_FOLDER}"
        )
        logging.warning(
            "If you continue these WILL BE DELETED, Please make sure you have them saved."
        )
        logging.warning("!" * 80)
        response = input("Continue? (Y/N): ").strip().lower()
        if response != "y":
            logging.info("Aborting...")
            exit(0)
        else:
            shutil.rmtree(
                Path(Config.COMBINED_FOLDER)
            )  # Delete everything including the directory
            Path(Config.COMBINED_FOLDER).mkdir()

    extraction = Extract(Config)
    batch = BatchNimrod(Config)
    timeseries = GenerateTimeseries(Config, locations)

    start = time.time()
    logging.info(
        "Starting interleaved processing of GZ files -> DAT -> ASC -> Timeseries"
    )

    # Get list of all tar files
    all_tar_files = [f for f in os.listdir(Config.TAR_TOP_FOLDER) if f.endswith(".tar")]
    all_tar_files.sort()
    total_tars = len(all_tar_files)
    files_per_tar = 288
    estimated_total_files = total_tars * files_per_tar
    logging.info(f"Found {total_tars} tar files to process")

    # Process in batches
    for i in range(0, total_tars, Config.BATCH_SIZE):
        batch_files = all_tar_files[i : i + Config.BATCH_SIZE]
        logging.info(
            f"Processing batch {i // Config.BATCH_SIZE + 1}: {len(batch_files)} tar files"
        )

        # Initialize results structure for this batch
        results = {loc[0]: {"dates": [], "values": []} for loc in locations}

        # 1. Extract batch (TAR -> GZ)
        logging.info("Extracting tar files for batch")
        extraction.extract_tar_batch(batch_files)

        gz_files_to_process = []
        for tar_file in batch_files:
            extract_folder = Path(Config.GZ_TOP_FOLDER, tar_file.replace(".tar", ""))
            if extract_folder.exists():
                for root, _, files in os.walk(extract_folder):
                    for file in files:
                        if file.endswith(".gz"):
                            gz_files_to_process.append(Path(root, file))

        total_files = len(gz_files_to_process)
        logging.info(f"Found {total_files} GZ files to process concurrently...")

        with concurrent.futures.ThreadPoolExecutor() as executor:
            future_to_file = {
                executor.submit(process_pipeline, gz_file): gz_file
                for gz_file in gz_files_to_process
            }

            completed_count = 0
            try:
                for future in concurrent.futures.as_completed(future_to_file):
                    file_results = future.result()
                    if file_results:
                        for res in file_results:
                            zone_id = res["zone_id"]
                            results[zone_id]["dates"].append(res["date"])
                            results[zone_id]["values"].append(res["value"])

                    completed_count += 1
                    if completed_count % 100 == 0:
                        files_processed_previous = i * files_per_tar
                        files_processed_so_far = (
                            files_processed_previous + completed_count
                        )

                        elapsed_time = time.time() - start
                        rate_per_second = files_processed_so_far / elapsed_time

                        remaining_files = estimated_total_files - files_processed_so_far

                        if rate_per_second > 0:
                            eta_seconds = remaining_files / rate_per_second

                            if eta_seconds < 60:
                                eta_str = f"{int(eta_seconds)}s"
                            elif eta_seconds < 3600:
                                eta_str = f"{int(eta_seconds // 60)}m {int(eta_seconds % 60)}s"
                            else:
                                eta_str = f"{int(eta_seconds // 3600)}h {int((eta_seconds % 3600) // 60)}m"
                        else:
                            eta_str = "Unknown"

                        logging.info(f"""Progress: {files_processed_so_far}/{estimated_total_files} files ({files_processed_so_far / estimated_total_files * 100:.1f}%)
    Speed: {rate_per_second * 60:.2f} files/min. ETA: {eta_str}""")
            except KeyboardInterrupt:
                logging.warning(
                    "KeyboardInterrupt received. Cancelling pending tasks..."
                )
                executor.shutdown(wait=False, cancel_futures=True)
                raise

        logging.info("Appending batch results to CSV files...")
        timeseries.append_results_to_csv(results, locations)

        # Cleanup GZ folders for this batch
        # We loop through batch_files again to delete the folders we created
        for tar_file in batch_files:
            extract_folder = Path(Config.GZ_TOP_FOLDER, tar_file.replace(".tar", ""))
            if extract_folder.exists():
                try:
                    shutil.rmtree(extract_folder)
                except OSError as e:
                    logging.warning(f"Failed to remove GZ folder {extract_folder}: {e}")
    end = time.time()
    elapsed_time = end - start

    if elapsed_time < 60:
        elapsed_time_str = f"{int(elapsed_time)}s"
    elif elapsed_time < 3600:
        elapsed_time_str = f"{int(elapsed_time // 60)}m {int(elapsed_time % 60)}s"
    else:
        elapsed_time_str = (
            f"{int(elapsed_time // 3600)}h {int((elapsed_time % 3600) // 60)}m"
        )

    logging.info(f"All Complete total time {elapsed_time_str}")
