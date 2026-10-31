# Module Upload to Databricks


# This module was AI suggestion because will be used several times and also combines shell
# scripting with subprocess.

# This file aims to: 

#   - Upload the given structure of the local data to its Unity Catalog volume in Databricks
#   - Check that every local file of each asset was uploaded
#   - Log the result of each upload


# Libraries

import time
import subprocess
from pathlib import Path
from datetime import datetime
 
 
 
# Class: Volume Uploader
 
class VolumeUploader():
 
    # Class Constructor - Attributes initialization
 
    def __init__(self, layer, local_root, volume_root, assets, logs_path):
 
        self.layer = layer
        self.local_root = local_root
        self.volume_root = volume_root
        self.assets = assets
        self.logs_path = logs_path
 
    # Method 1: upload_asset
 
    def upload_asset(self, symbol):
 
        source = Path(self.local_root) / f"symbol={symbol}"
        target = f"{self.volume_root}/symbol={symbol}"
 
        # The CLI is called directly withput Shell
        # cp -r copies the contents of source into target: target/year=.../month=.../data.parquet
 
        result = subprocess.run(
            ["databricks", "fs", "cp", "-r", str(source), target, "--overwrite"],
                capture_output = True, text = True)
 
        # The CLI prints one "source -> target" line per copied file
 
        local_files = len(list(source.rglob("*.parquet")))
        uploaded_files = result.stdout.count(" -> ")
 
        ok = result.returncode == 0 and uploaded_files == local_files
 
        return ok, local_files, uploaded_files, result.stderr.strip()
 
    # Method 2: run (main orchestrator)
 
    def run(self):
 
        print(f"Starting: Upload of {self.layer} to {self.volume_root}")
 
        # Start timer
        
        start_time = time.perf_counter()
 
        lines = []
 
        for symbol in self.assets:
 
            ok, local_files, uploaded_files, error = self.upload_asset(symbol)
 
            status = "OK" if ok else "FAILED"
 
            lines.append(f"{symbol:<6} | files: {uploaded_files:>3} of {local_files:>3} | {status}")
 
            print(lines[-1])
 
            if not ok:
 
                print(error)
 
        # End timer
 
        end_time = time.perf_counter()
        total_time = end_time - start_time
 
        print(f"Execution time: {total_time:.3f} s")
        print("Finished: Upload")
 
        # Logs handling
 
        if self.logs_path:
 
            Path(self.logs_path).parent.mkdir(parents = True, exist_ok = True)
 
            with open(self.logs_path, "a", encoding = "utf-8") as file:
 
                file.write("------------------  UPLOAD METADATA ------------------ " + "\n")
                file.write("\n")
                file.write(f"Execution date: {datetime.now():%Y-%m-%d %H:%M}" + "\n")
                file.write(f"Layer: {self.layer} | from: {self.local_root} | to: {self.volume_root}" + "\n")
                file.write(f"Execution time: {total_time:.3f} s" + "\n")
                file.write("\n")
                file.write("Summary per asset:" + "\n")
 
                for line in lines:
 
                    file.write(line + "\n")
 
                file.write("--" * 50 + "\n")
 
        else:
 
            print("LOGS_PATH is not set in .env")