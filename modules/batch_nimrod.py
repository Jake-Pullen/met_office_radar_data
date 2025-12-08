from modules.nimrod import Nimrod
import os
from pathlib import Path
import logging
import concurrent.futures



class BatchNimrod:
    def __init__(self, config) -> None:
        self.config = config

    def _process_single_file(self, in_file):
        """Process a single Nimrod DAT file.
        
        Args:
            in_file (str): Filename of the DAT file.
            
        Returns:
            bool: True if successful, False otherwise.
        """
        in_file_full = Path(self.config.DAT_TOP_FOLDER, in_file)
        
        try:
            # We need to open the file here, inside the thread
            with open(in_file_full, "rb") as f:
                image = Nimrod(f)
                
                out_file_name = f"{image.get_validity_time()}.asc"
                out_file_path = Path(self.config.ASC_TOP_FOLDER, out_file_name)

                with open(out_file_path, "w") as outfile:
                    image.extract_asc(outfile)

            if self.config.delete_dat_after_processing:
                os.remove(in_file_full)

            logging.debug(f"Successfully processed: {in_file_full}")
            return True

        except Nimrod.HeaderReadError as e:
            logging.error(f"Failed to read file {in_file_full}, is it corrupt?")
            logging.error(e)
            return False
        except Nimrod.PayloadReadError as e:
            logging.error(f"Failed to load the raster data in {in_file_full}")
            logging.error(e)
            return False
        except Exception as e:
            logging.error(f"Unexpected error processing {in_file_full}: {e}")
            return False

    def process_nimrod_files(self) -> None:
        """
        Process all Nimrod files in the input directory concurrently, applying bounding box clipping
        and exporting to ASC format.

        This function reads all files from DAT_TOP_FOLDER, applies the appropriate bounding
        box for each area, and exports clipped raster data to OUT_TOP_FOLDER.
        """
        # Read all file names in the folder
        files_to_process = [f for f in os.listdir(Path(self.config.DAT_TOP_FOLDER)) if not f.startswith('.')]
        total_files = len(files_to_process)

        logging.info(f"Processing {total_files} files concurrently...")
        
        with concurrent.futures.ThreadPoolExecutor() as executor:
            # Submit all tasks
            future_to_file = {
                executor.submit(self._process_single_file, in_file): in_file 
                for in_file in files_to_process
            }
            
            completed_count = 0
            try:
                for future in concurrent.futures.as_completed(future_to_file):
                    completed_count += 1
                    if completed_count % 10 == 0:
                        logging.info(f'processed {completed_count} out of {total_files} files')
            except KeyboardInterrupt:
                logging.warning("KeyboardInterrupt received. Cancelling pending tasks...")
                executor.shutdown(wait=False, cancel_futures=True)
                raise

