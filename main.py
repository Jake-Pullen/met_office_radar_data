import logging
import yaml

CONFIG_FILE = "config.yaml"

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

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
    

os.makedirs(Path(OUT_TOP_FOLDER), exist_ok=True)
os.makedirs(Path(CSV_TOP_FOLDER), exist_ok=True)




# if __name__ == "__main__":
#     start = time.time()
#     process_nimrod_files()
#     end = time.time()
#     elapsed_time = end - start
#     logging.info(f"Processing completed in {elapsed_time:.2f} seconds")