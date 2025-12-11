import tarfile
import gzip
import shutil
import os
from pathlib import Path


class Extract:
    # Directory containing .tar files
    def __init__(self, Config):
        self.config = Config

    def _extract_tar(self):
        for tar_file in os.listdir(self.config.TAR_TOP_FOLDER):
            # only handle .tar files
            if not tar_file.endswith(".tar"):
                pass

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

    def _extract_gz(self):
        for root, _, files in os.walk(self.config.GZ_TOP_FOLDER):
            for file in files:
                # only handle .gz files
                if not file.endswith(".dat.gz"):
                    pass  # adjust if extension differs
                gz_path = Path(root, file)
                dat_path = Path(self.config.DAT_TOP_FOLDER, file.replace(".gz", ""))

                # Unzip .gz file
                with gzip.open(gz_path, "rb") as f_in:
                    with open(dat_path, "wb") as f_out:
                        shutil.copyfileobj(f_in, f_out)

                if self.config.delete_gz_after_processing:
                    os.remove(gz_path)

        try:
            shutil.rmtree(self.config.GZ_TOP_FOLDER)
            print("processing complete and GZ files deleted")
        except Exception as e:
            print(str(e))
            print(
                f"processing complete but GZ folder delete failed. Please delete manually ({self.config.GZ_TOP_FOLDER})"
            )

    def run_extraction(self):
        self._extract_tar()
        self._extract_gz()
