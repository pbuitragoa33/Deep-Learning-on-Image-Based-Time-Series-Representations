# Execute the uploading to a data layer to Databricks


# Libraries

import os
import json
import sys
from dotenv import load_dotenv
from pathlib import Path
import subprocess

# Add src directory to Python path

SRC_PATH = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SRC_PATH))

from utils.upload_to_databricks import VolumeUploader


# Execution 

if __name__ == "__main__":

    load_dotenv()

    # Layer selection

    layer = sys.argv[1] if len(sys.argv) > 1 else "bronze"

    roots = {
        "bronze": (os.getenv("BRONZE_ROOT"), os.getenv("BRONZE_VOLUME")),
        "silver": (os.getenv("SILVER_ROOT"), os.getenv("SILVER_VOLUME"))
    }

    if layer not in roots:

        raise ValueError(f"Invalid layer: {layer}")

    local_root, volume_root = roots[layer]

    # Load assets

    with open(
        os.getenv("DOWNLOAD_CONFIGS_PATH"), mode = "r", encoding = "utf-8") as configs_download:
        
        params = json.load(configs_download
    )

    # Initialize uploader

    VU = VolumeUploader(
        layer = layer,
        local_root = local_root,
        volume_root = volume_root,
        assets = params["assets"],
        logs_path = os.getenv("LOGS_PATH")
    )

    VU.run()