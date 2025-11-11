from modules.nimrod import Nimrod
import os
from pathlib import Path
import logging


class BatchNimrod():
    def __init__(self, config) -> None:
        self.config = config

    def process_nimrod_files(self) -> None:
        """
        Process all Nimrod files in the input directory, applying bounding box clipping
        and exporting to ASC format.

        This function reads all files from IN_TOP_FOLDER, applies the appropriate bounding
        box for each area, and exports clipped raster data to OUT_TOP_FOLDER.
        """
        # Read all file names in the folder
        files_to_process = [f for f in os.listdir(Path(self.config.IN_TOP_FOLDER))]

        logging.info(f"Processing {len(files_to_process)} files...")

        for in_file in os.listdir(Path(self.config.IN_TOP_FOLDER)):
            in_file_full = Path(self.config.IN_TOP_FOLDER, in_file)

            try:
                image = Nimrod(open(in_file_full, "rb"))

                out_file_name = f"{image.get_validity_time()}.asc"
                out_file_path = Path(self.config.OUT_TOP_FOLDER, out_file_name)

                with open(out_file_path, "w") as outfile:
                    image.extract_asc(outfile)
                logging.debug(f"Successfully processed: {in_file_full}")

            except Nimrod.HeaderReadError as e:
                logging.error(f"Failed to read file {in_file_full}, is it corrupt?")
                logging.error(e)
                continue
            except Nimrod.PayloadReadError as e:
                logging.error(f"Failed to load the raster data in {in_file_full}")
                logging.error(e)
                continue