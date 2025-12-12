import tarfile
import gzip
import shutil
import os
from pathlib import Path
import concurrent.futures


class Extract:
    # Directory containing .tar files
    def __init__(self, Config):
        self.config = Config

    def extract_tar_batch(self, tar_files):
        for tar_file in tar_files:
            tar_path = Path(self.config.TAR_TOP_FOLDER, tar_file)

            # Create a folder for extracted tar contents
            extract_folder = Path(
                self.config.GZ_TOP_FOLDER, tar_file.replace(".tar", "")
            )
            Path(extract_folder).mkdir(exist_ok=True)

            # Extract .tar file
            with tarfile.open(tar_path, "r") as tar:
                tar.extractall(path=extract_folder)

            if self.config.delete_tar_after_processing:
                os.remove(tar_path)

    def process_single_gz(self, gz_path, dat_path):
        try:
            with gzip.open(gz_path, "rb") as f_in:
                with open(dat_path, "wb") as f_out:
                    shutil.copyfileobj(f_in, f_out)

            if self.config.delete_gz_after_processing:
                os.remove(gz_path)
        except Exception as e:
            print(f"Error extracting {gz_path}: {e}")

    def extract_gz_batch(self):
        gz_tasks = []
        for root, _, files in os.walk(self.config.GZ_TOP_FOLDER):
            for file in files:
                # only handle .gz files
                if not file.endswith(".dat.gz"):
                    continue

                gz_path = Path(root, file)
                dat_path = Path(self.config.DAT_TOP_FOLDER, file.replace(".gz", ""))
                gz_tasks.append((gz_path, dat_path))

        print(f"Extracting {len(gz_tasks)} gz files concurrently...")

        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = [
                executor.submit(self.process_single_gz, gz_path, dat_path)
                for gz_path, dat_path in gz_tasks
            ]
            concurrent.futures.wait(futures)

        try:
            shutil.rmtree(self.config.GZ_TOP_FOLDER)
            # Recreate the folder for the next batch
            Path(self.config.GZ_TOP_FOLDER).mkdir(exist_ok=True)
            print("processing complete and GZ files deleted")
        except Exception as e:
            print(str(e))
            print(
                f"processing complete but GZ folder delete failed. Please delete manually ({self.config.GZ_TOP_FOLDER})"
            )
