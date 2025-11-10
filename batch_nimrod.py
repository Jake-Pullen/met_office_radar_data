from nimrod import Nimrod
import os
from pathlib import Path
import re
import logging
import yaml
import time

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

BOUNDING_BOX_INFO = {
    "BRISCS": (607000, 608000, 217000, 218000),
    "WINTSC": (499000, 500000, 416000, 417000),
}

# TODO: The dat files im fairly sure are duplicated as it is the whole uk area, not area specific. need to change it
IN_TOP_FOLDER = "./dat_files"
OUT_TOP_FOLDER = "./asc_files"
CONFIG_FILE = "config.yaml"


def get_datetime(file_name: str) -> str:
    """
    Extract datetime from a filename using regex pattern matching.

    Args:
        file_name (str): The name of the file to extract datetime from.

    Returns:
        str: The extracted datetime in YYYYMMDDHHMM format, or 'date_not_found' if no match.
    """
    pattern = r"(\d{8})(\d{4})"
    match = re.search(pattern, file_name)
    if match:
        date_part = match.group(1)  # YYYYMMDD
        time_part = match.group(2)  # HHMM
        return f"{date_part}{time_part}"
    else:
        return "date_not_found"


def load_config() -> dict:
    """
    Load configuration from YAML file.

    Returns:
        dict: Configuration dictionary containing bounding box information.

    Raises:
        FileNotFoundError: If the config.yaml file is not found.
        yaml.YAMLError: If there's an error parsing the YAML file.
    """
    try:
        with open(CONFIG_FILE, "r") as file:
            config = yaml.safe_load(file)
            return config.get("bounding_box_info", {})
    except FileNotFoundError:
        logging.error(
            f"Config file {CONFIG_FILE} not found. Using default configuration."
        )
        return {}
    except yaml.YAMLError as e:
        logging.error(f"Error parsing YAML file: {e}")
        return {}


def process_nimrod_files() -> None:
    """
    Process all Nimrod files in the input directory, applying bounding box clipping
    and exporting to ASC format.

    This function reads all files from IN_TOP_FOLDER, applies the appropriate bounding
    box for each area, and exports clipped raster data to OUT_TOP_FOLDER.
    """
    # Read all file names in the folder
    files_to_process = [f for f in os.listdir(Path(IN_TOP_FOLDER))]

    logging.info(f"Processing {len(files_to_process)} files...")

    os.makedirs(Path(OUT_TOP_FOLDER), exist_ok=True)

    for in_file in os.listdir(Path(IN_TOP_FOLDER)):
        timestamp = get_datetime(in_file)
        out_file_name = f"{timestamp}.asc"
        out_file_path = Path(OUT_TOP_FOLDER, out_file_name)
        in_file_full = Path(IN_TOP_FOLDER, in_file)

        try:
            image = Nimrod(open(in_file_full, "rb"))
            with open(out_file_path, "w") as outfile:
                image.extract_asc(outfile)
            # logging.info(f"Successfully processed: {in_file_full}")

        except Nimrod.HeaderReadError as e:
            logging.error(f"Failed to read file {in_file_full}, is it corrupt?")
            logging.error(e)
            continue
        except Nimrod.PayloadReadError as e:
            logging.error(f"Failed to load the raster data in {in_file_full}")
            logging.error(e)
            continue
        except Nimrod.BboxRangeError as e:
            logging.error(
                "Bounding Box out of range. Given bounding box: {bounding_box}"
            )
            logging.error(e)
            # Skips the whole area as bounding box will be out of bounds for all files
            break


if __name__ == "__main__":
    start = time.time()
    process_nimrod_files()
    end = time.time()
    elapsed_time = end - start
    logging.info(f"Processing completed in {elapsed_time:.2f} seconds")
