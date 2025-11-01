from nimrod import Nimrod
import os
from pathlib import Path
import re
import logging
import yaml

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

BOUNDING_BOX_INFO = {
    "BRISCS": (607000, 608000, 217000, 218000),
    "WINTSC": (499000, 500000, 416000, 417000),
}

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
    # Load configuration
    bounding_box_info = load_config()

    # Use default if config is empty
    if not bounding_box_info:
        bounding_box_info = {
            "BRISCS": (607000, 608000, 217000, 218000),
            "WINTSC": (499000, 500000, 416000, 417000),
        }

    # read all file names in the folder
    area_folders = os.listdir(IN_TOP_FOLDER)

    for area in area_folders:
        bounding_box = bounding_box_info.get(area, (0, 0, 0, 0))
        logging.info(f"Processing area: {area}, bounding box: {bounding_box}")
        xmin, xmax, ymin, ymax = bounding_box
        os.makedirs(Path(OUT_TOP_FOLDER, area), exist_ok=True)

        for in_file in os.listdir(Path(IN_TOP_FOLDER, area)):
            timestamp = get_datetime(in_file)
            out_file_name = f"{timestamp}_{area}.asc"
            out_file_path = Path(OUT_TOP_FOLDER, area, out_file_name)
            in_file_full = Path(IN_TOP_FOLDER, area, in_file)

            try:
                image = Nimrod(open(in_file_full, "rb"))
                image.apply_bbox(xmin, xmax, ymin, ymax)
                # image.query() # prints out file_details
                with open(out_file_path, "w") as outfile:
                    image.extract_asc(outfile)
                logging.info(f"Successfully processed: {in_file_full}")

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
                    f"Bounding Box out of range. Given bounding box: {bounding_box}"
                )
                logging.error(e)
                # Skips the whole area as bounding box will be out of bounds for all files
                break


if __name__ == "__main__":
    process_nimrod_files()
