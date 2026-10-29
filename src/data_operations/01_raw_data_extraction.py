# 1. Raw Data Extraction


# This file aims to: 

#   - Download the specified assets, schema and parameters from Databento
#   - Follow a tidy folder structure 
#   - The storage is set to be done in Databricks


# Libraries

import os 
import time 
import json
import databento as db
import polars as pl
from dotenv import load_dotenv
from pathlib import Path
from datetime import datetime, date



# Class: Raw Data Extractor

class RawDataExtractor():

    # Class Constructor - Attributes initialization

    def __init__(self, db_key, start_date, end_date, assets, dataset, schema, root, logs_path):

        self.db_key = db_key
        self.client = db.Historical(db_key)
        self.start_date = start_date
        self.end_date = end_date
        self.assets = assets
        self.dataset = dataset
        self.schema = schema
        self.root = root
        self.logs_path = logs_path

    # Method 1: generate_months

    def generate_month_windows(self):

        # Generate each month between the start date and the end date

        st_year, st_month, st_day = self.start_date.split("-")
        en_year, en_month, en_day = self.end_date.split("-")
        
        months = pl.date_range(start = date(int(st_year), int(st_month), int(st_day)), 
                      end = date(int(en_year), int(en_month), int(en_day)),
                      interval = "1mo", closed = "both", eager = True)

        list_of_months = months.to_list()

        month_windows = list(zip(list_of_months[:-1], list_of_months[1:]))

        return month_windows


    # Method 2: build_file_path

    def build_file_path(self, symbol, year, month):

        path = (
            Path(self.root)
            / f"symbol={symbol}"
            / f"year={year}"
            / f"month={month:02d}"
            / "data.parquet"
        )

        return path


    # Method 3: pending_tasks

    def pending_tasks(self):

        tasks = []

        for start, end in self.generate_month_windows():

            for symbol in self.assets:

                p = self.build_file_path(symbol = symbol, year = start.year, month = start.month)

                if not p.exists():

                    tasks.append((symbol, start, end, p))

        return tasks


    # Method 4: cost_estimation

    def cost_estimation(self, tasks):

        total = 0

        for symbol, start, end, path in tasks:

            print(symbol, start, end)

            cost = self.client.metadata.get_cost(dataset = self.dataset,
                                                 symbols = symbol,
                                                 schema = self.schema,
                                                 start = start, end = end)
            total += cost

        return total

 # Method 5: degraded_days
 
    def degraded_days(self):
 
        # The condition is reported per dataset and day (not per symbol), so a single call covers the whole range
 
        conditions = self.client.metadata.get_dataset_condition(dataset = self.dataset,
                                                                start_date = self.start_date,
                                                                end_date = self.end_date)
 
        days = [date.fromisoformat(c["date"]) for c in conditions if c["condition"] == "degraded"]
 
        return days
 
    # Method 6: download_task
 
    def download_task(self, symbol, start, end):
 
        store = self.client.timeseries.get_range(dataset = self.dataset,
                                                 symbols = symbol,
                                                 schema = self.schema,
                                                 start = start,
                                                 end = end)
 
        df = pl.from_pandas(store.to_df().reset_index())
 
        return df
 
    # Method 7: save_task
 
    def save_task(self, df, path):
 
        df = df.drop("symbol")
 
        path.parent.mkdir(parents = True, exist_ok = True)
 
        df.write_parquet(path)
 
        return None
 
    # Method 8: summary_lines
 
    def summary_lines(self, summary):
 
        # One line per asset downloaded in this execution
 
        lines = []
 
        for symbol, info in summary.items():
 
            if info["months"] == 0 and info["empty"] == 0:
 
                continue
 
            days = ", ".join(str(d) for d in info["degraded"]) if info["degraded"] else "none"
 
            lines.append(f"{symbol:<6} | months: {info['months']:>3} | empty: {info['empty']:>3} | "
                         f"rows: {info['rows']:>10,} | degraded days ({len(info['degraded'])}): {days}")
 
        total_rows = sum(info["rows"] for info in summary.values())
 
        lines.append(f"Total rows: {total_rows:,}")
 
        return lines
 
    # Method 9: run (main orchestrator)
 
    def run(self):
 
        print("\n" + "-" * 50)
        print("STARTING DATA DOWNLOAD")
        print("-" * 50 + "\n")
 
        # Start timer
        
        start_time = time.perf_counter()
 
        # 1. Window generator
 
        windows = self.generate_month_windows()
 
        # 2. Tasks and cost estimations
 
        tasks = self.pending_tasks()
        cost = self.cost_estimation(tasks)
 
        print(f"Windows: {len(windows)} | first: {windows[0]} | last: {windows[-1]}")
        print(f"Pending tasks: {len(tasks)}")
        print(f"Max estimated cost: $ {cost:.2f}")
 
        # 3. Degraded days of the dataset in the requested range
 
        degraded = self.degraded_days()
 
        print(f"Degraded days in range: {len(degraded)}")
 
        # 4. Download tasks
 
        summary = {symbol: {"months": 0, "empty": 0, "rows": 0, "degraded": []} for symbol in self.assets}
 
        for symbol, start, end, path in tasks:
 
            df = self.download_task(symbol, start, end)
 
            if df.height == 0:
 
                summary[symbol]["empty"] += 1
 
                print(f"{symbol} {start}: empty, skipped")
 
                continue
 
            self.save_task(df, path)
 
            summary[symbol]["months"] += 1
            summary[symbol]["rows"] += df.height
            summary[symbol]["degraded"] += [d for d in degraded if start <= d < end]
 
            print(f"{symbol} {start}: {df.height} rows")
 
        pending_after = len(self.pending_tasks())
 
        print(f"Pending after download: {pending_after}")
 
        # 5. Summary per asset
 
        lines = self.summary_lines(summary)
 
        print("Summary per asset:")
 
        for line in lines:
 
            print(line)
 
        # End timer
 
        end_time = time.perf_counter()
        total_time = end_time - start_time
 
        print(f"Execution time: {total_time:.3f} s")
        
        print("\n" + "-" * 50)
        print("FINISHING DATA DOWNLOAD")
        print("-" * 50 + "\n")
 
        # Logs handling
 
        if self.logs_path:
 
            Path(self.logs_path).parent.mkdir(parents = True, exist_ok = True)
 
            with open(self.logs_path, "a", encoding = "utf-8") as file:
 
                file.write("------------------  DOWNLOAD METADATA ------------------ " + "\n")
                file.write("\n")
                file.write(f"Execution date: {datetime.now():%Y-%m-%d %H:%M}" + "\n")
                file.write(f"Number of windows: {len(windows)}" + "\n")
                file.write(f"First window: {windows[0]} | Last window: {windows[-1]}" + "\n")
                file.write(f"Execution time: {total_time:.3f} s" + "\n")
                file.write(f"Estimated cost: {cost:.2f}" + "\n")
                file.write("\n")
                file.write("Summary per asset:" + "\n")
 
                for line in lines:
 
                    file.write(line + "\n")
 
                file.write("--" * 50 + "\n")
 
        else:
 
            print("LOGS_PATH is not set in .env")
 
 
 
# Execution
 
 
if __name__ == "__main__":
 
    # Load the environment variable
 
    load_dotenv()
 
    # API Key of Databento
    databento_api_key = os.getenv("DATABENTO_API_KEY")
 
    # Config file path
    download_configs_path = os.getenv("DOWNLOAD_CONFIGS_PATH")
 
    # Output root
    output_root = os.getenv("OUTPUT_ROOT")
 
    # Execution logs path
    logs_path = os.getenv("LOGS_PATH")
 
    # Open the configs file
 
    with open(file = download_configs_path, mode = "r", encoding = "utf-8") as configs_download:
 
        params = json.load(configs_download)
 
    # Class instance
 
    RDE = RawDataExtractor(
        db_key = databento_api_key,
        start_date = params["start_date"],
        end_date = params["end_date"],
        assets = params["assets"],
        dataset = params["dataset"],
        schema = params["schema"],
        root = output_root,
        logs_path = logs_path
    )
 
    # Run the pipeline
 
    RDE.run()