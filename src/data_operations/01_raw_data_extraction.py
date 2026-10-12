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
import numpy as np
from dotenv import load_dotenv
import tempfile
from pathlib import Path
from datetime import datetime, date


# Class: Raw Data Extractor

class RawDataExtractor():

    # Class Constructor - Attributes initialization

    def __init__(self, db_key, start_date, end_date, assets, dataset, schema, root):

        self.db_key = db_key
        self.client = db.Historical(db_key)
        self.start_date = start_date
        self.end_date = end_date
        self.assets = assets
        self.dataset = dataset
        self.schema = schema
        self.root = root

    # Method 1: generate_months

    def generate_month_windows(self):

        # Generate each month between the start date and the end date

        st_year, st_month, st_day = start_date.split("-")
        en_year, en_month, en_day = end_date.split("-")
        
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
            / f"dataset={self.dataset}"
            / f"schema={self.schema}"
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

    # Method 5: download_task



        

        

    

        

if __name__ == "__main__":

    # Execution


    # Load the environment variable

    load_dotenv()

    # API Key of Databento
    databento_api_key = os.getenv("DATABENTO_API_KEY")

    # Config file path
    download_configs_path = os.getenv("DOWNLOAD_CONFIGS_PATH")

    # Output root
    root = os.getenv("OUTPUT_ROOT")

    # Open the configs file (dict) and set the configurations as variables

    with open(file = download_configs_path, mode = "r", encoding = "utf-8") as configs_download:

        params = json.load(configs_download)

    start_date = params["start_date"]
    end_date = params["end_date"]
    assets = params["assets"]
    dataset = params["dataset"]
    schema = params["schema"]


    # Class instannce

    RDE = RawDataExtractor(db_key = databento_api_key,
                           start_date = start_date, 
                           end_date = end_date, 
                           assets = assets,
                           dataset = dataset,
                           schema = schema,
                           root = root)

    # 1. Window generator

    windows = RDE.generate_month_windows()

    # 2. Tasks and cost estimations

    tasks = RDE.pending_tasks()

    print(f"Windows: {len(windows)} | first: {windows[0]} | last: {windows[-1]}")  

    print(f"Pending tasks: {len(tasks)}")
    print(f"Estimated cost: $ {RDE.cost_estimation(tasks):.2f}")
    print(f"Cost per month of a given asset: ${(RDE.cost_estimation(tasks) / len(tasks)):.5f}")

