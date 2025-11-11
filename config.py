import yaml
import logging

class Config:
    def __init__(self) -> None:
        self.IN_TOP_FOLDER = "./dat_files"
        self.OUT_TOP_FOLDER = "./asc_files"
        self.CSV_TOP_FOLER = "./csv_files"
        self.AREAS_FILE = 'areas.csv'

    

    def load_areas(self) -> dict:
        """
        Load configuration from YAML file.

        Returns:
            dict: Configuration dictionary containing bounding box information.

        Raises:
            FileNotFoundError: If the config.yaml file is not found.
            yaml.YAMLError: If there's an error parsing the YAML file.
        """
        try:
            with open(, "r") as file:
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

