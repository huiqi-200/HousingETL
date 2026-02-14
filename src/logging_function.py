import logging
import json
from pathlib import Path
import os

# create a shared logger for the entire project
logger = logging.getLogger("housing_etl")
logger.setLevel(logging.INFO)

# always configure handlers exactly once
if not logger.handlers:
    # console handler
    console = logging.StreamHandler()
    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
    console.setFormatter(formatter)
    logger.addHandler(console)

    # attempt to add a file handler based on config
    try:
        config_path = Path(__file__).parent / "config.json"
        with open(config_path, "r") as f:
            cfg = json.load(f)
        logs_relative = cfg.get("FolderPaths", {}).get("LogsFolderPath")
        if logs_relative:
            logs_dir = Path(os.getcwd()) / logs_relative
            logs_dir.mkdir(parents=True, exist_ok=True)
            file_handler = logging.FileHandler(logs_dir / "housing_etl.log")
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)
    except Exception:
        # if anything goes wrong reading config/opening file, skip file logging
        pass


