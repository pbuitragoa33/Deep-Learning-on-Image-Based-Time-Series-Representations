# 2. Processed Data Curing (Silver)


# This file aims to:

#   - Read the raw 1-minute bars (bronze) of each asset from the local copy not from the volume
#   - Keep only the regular session (09:30 - 16:00 New York time, NYSE calendar)
#   - Adjust prices and volume for splits
#   - Fill (ffill) the minutes without trades with the last known price and flag them
#   - Flag the degraded days reported by Databento
#   - Save the result (silver) with the same folder structure as bronze
#   - Report the coverage (share of real bars) per asset and year, and the largest daily moves
#   - Silver is rebuilt from bronze on every run (bronze is never modified)


# Libraries

import os
import time
import json
import polars as pl
import pandas_market_calendars as mcal
from dotenv import load_dotenv
from pathlib import Path
from datetime import datetime, date, timedelta



# Class: Processed Data Curator

class ProcessedDataCurator():

    # Class Constructor - Attributes initialization

    def __init__(self, start_date, end_date, assets, splits, degraded_days, bronze_root, silver_root, logs_path):

        self.start_date = start_date
        self.end_date = end_date
        self.assets = assets
        self.splits = splits
        self.degraded_days = [date.fromisoformat(d) for d in degraded_days]
        self.bronze_root = bronze_root
        self.silver_root = silver_root
        self.logs_path = logs_path

    # Method 1: build_minute_grid

    def build_minute_grid(self):

        # Every minute of the regular session for each NYSE trading day in [start_date, end_date)
        # Holidays and early closes (e.g. 13:00 the day after Thanksgiving) come from the calendar
        # A 1-minute bar is stamped at its start, so the session is 09:30 ... 15:59 (390 bars)

        nyse = mcal.get_calendar("NYSE")

        last_day = date.fromisoformat(self.end_date) - timedelta(days = 1)

        schedule = nyse.schedule(start_date = self.start_date, end_date = last_day)

        sessions = pl.from_pandas(schedule[["market_open", "market_close"]])

        grid = (
            sessions
            .select(pl.datetime_ranges("market_open", "market_close", interval = "1m", closed = "left").alias("ts_event"))
            .explode("ts_event")
            .with_columns(pl.col("ts_event").dt.cast_time_unit("us"))
        )

        # New York time and date, used for the split dates, the partitions and later steps

        grid = grid.with_columns(pl.col("ts_event").dt.convert_time_zone("America/New_York").alias("ts"))
        grid = grid.with_columns(pl.col("ts").dt.date().alias("date"))

        return grid


    # Method 2: build_file_path

    def build_file_path(self, root, symbol, year, month):

        path = (
            Path(root)
            / f"symbol={symbol}"
            / f"year={year}"
            / f"month={month:02d}"
            / "data.parquet"
        )

        return path


    # Method 3: load_bronze

    def load_bronze(self, symbol):

        files = sorted((Path(self.bronze_root) / f"symbol={symbol}").rglob("*.parquet"))

        columns = ["ts_event", "open", "high", "low", "close", "volume"]

        bars = pl.concat([pl.read_parquet(file, columns = columns) for file in files])

        bars = bars.with_columns(pl.col("ts_event").dt.cast_time_unit("us"),
                                 pl.col("volume").cast(pl.Float64))

        return bars


    # Method 4: adjust_splits (AI suggested method)

    def adjust_splits(self, df, symbol):

        # factor = new shares per old share (2:1 split -> 2, 1:8 reverse split -> 0.125)
        # Bars before the ex-date: prices / factor, volume * factor
        # Several splits of the same asset accumulate because each one is applied on top of the previous

        for split in self.splits:

            if split["symbol"] != symbol:

                continue

            before = pl.col("date") < date.fromisoformat(split["ex_date"])
            factor = split["factor"]

            df = df.with_columns(
                [pl.when(before).then(pl.col(c) / factor).otherwise(pl.col(c)).alias(c) for c in ["open", "high", "low", "close"]]
                + [pl.when(before).then(pl.col("volume") * factor).otherwise(pl.col("volume")).alias("volume")]
            )

        return df


    # Method 5: fill_missing

    def fill_missing(self, df):

        # A minute without trades has no bar in bronze, so it comes empty from the grid
        # The close takes the last known price (only past data, no look-ahead), which can be
        # the previous day's close when the first minutes of a session have no trades

        df = df.with_columns(pl.col("close").is_null().alias("is_filled"))

        df = df.with_columns(pl.col("close").forward_fill())

        df = df.with_columns(pl.col("open").fill_null(pl.col("close")),
                             pl.col("high").fill_null(pl.col("close")),
                             pl.col("low").fill_null(pl.col("close")),
                             pl.col("volume").fill_null(0))

        # Minutes before the first trade of the whole history have nothing to fill from

        df = df.drop_nulls("close")

        return df


    # Method 6: build_silver

    def build_silver(self, grid, symbol):

        bars = self.load_bronze(symbol)

        # The left join keeps only the regular session: pre and post market bars are dropped

        df = grid.join(bars, on = "ts_event", how = "left").sort("ts_event")

        df = self.adjust_splits(df, symbol)

        df = self.fill_missing(df)

        df = df.with_columns(pl.col("date").is_in(self.degraded_days).alias("is_degraded"))

        df = df.select("ts", "date", "open", "high", "low", "close", "volume", "is_filled", "is_degraded")

        return df


    # Method 7: save_silver

    def save_silver(self, df, symbol):

        df = df.with_columns(pl.col("date").dt.year().alias("year"),
                             pl.col("date").dt.month().alias("month"))

        for (year, month), part in df.group_by(["year", "month"]):

            path = self.build_file_path(self.silver_root, symbol, year, month)

            path.parent.mkdir(parents = True, exist_ok = True)

            part.drop("year", "month").sort("ts").write_parquet(path)

        return None


    # Method 8: yearly_coverage

    def yearly_coverage(self, df, symbol):

        # Share of regular-session minutes with a real bar (not filled), in %

        coverage = (
            df.group_by(pl.col("date").dt.year().alias("year"))
            .agg(((1 - pl.col("is_filled").mean()) * 100).round(1).alias(symbol))
            .sort("year")
        )

        return coverage


    # Method 9: largest_moves

    def largest_moves(self, df, n = 3):

        # Largest close-to-close daily returns. A move close to +300% / -75% (1:4) or
        # -50% / +100% (2:1) means a split is missing or has the wrong date

        daily = df.group_by("date").agg(pl.col("close").last()).sort("date")

        daily = daily.with_columns((pl.col("close") / pl.col("close").shift(1) - 1).alias("ret")).drop_nulls("ret")

        top = daily.sort(pl.col("ret").abs(), descending = True).head(n)

        return " | ".join(f"{d} {r:+.1%}" for d, r in zip(top["date"], top["ret"]))


    # Method 10: days_without_trades

    def days_without_trades(self, df):

        # Sessions where every minute was filled: a missing bronze month would show up here
        # as many consecutive days carrying the same price

        days = df.group_by("date").agg(pl.col("is_filled").all()).filter(pl.col("is_filled"))

        return days.height


    # Method 11: run (main orchestrator)

    def run(self):

        print("Starting: Processed Data Curing")

        # Start timer
        start_time = time.perf_counter()

        # 1. Minute grid of the regular session

        grid = self.build_minute_grid()

        print(f"Trading days: {grid['date'].n_unique()} | minutes: {grid.height}")

        # 2. Silver per asset

        lines = []
        coverage = None

        for symbol in self.assets:

            df = self.build_silver(grid, symbol)

            self.save_silver(df, symbol)

            lines.append(f"{symbol:<6} | rows: {df.height:>9,} | filled: {df['is_filled'].mean():6.1%} | "
                         f"days without trades: {self.days_without_trades(df):>3} | "
                         f"largest daily moves: {self.largest_moves(df)}")

            print(lines[-1])

            table = self.yearly_coverage(df, symbol)

            coverage = table if coverage is None else coverage.join(table, on = "year")

        # 3. Coverage per asset and year

        coverage_text = coverage.to_pandas().to_string(index = False)

        print("Coverage of the regular session (% of minutes with a real bar):")
        print(coverage_text)

        # End timer

        end_time = time.perf_counter()
        total_time = end_time - start_time

        print(f"Execution time: {total_time:.3f} s")
        print("Finished: Processed Data Curing")

        # Logs handling

        if self.logs_path:

            Path(self.logs_path).parent.mkdir(parents = True, exist_ok = True)

            with open(self.logs_path, "a", encoding = "utf-8") as file:

                file.write("------------------  SILVER METADATA ------------------ " + "\n")
                file.write("\n")
                file.write(f"Execution date: {datetime.now():%Y-%m-%d %H:%M}" + "\n")
                file.write(f"Range: {self.start_date} to {self.end_date} | trading days: {grid['date'].n_unique()}" + "\n")
                file.write(f"Execution time: {total_time:.3f} s" + "\n")
                file.write("\n")
                file.write("Summary per asset:" + "\n")

                for line in lines:

                    file.write(line + "\n")

                file.write("\n")
                file.write("Coverage of the regular session (% of minutes with a real bar):" + "\n")
                file.write(coverage_text + "\n")
                file.write("--" * 50 + "\n")

        else:

            print("LOGS_PATH is not set in .env")



# Execution


if __name__ == "__main__":

    # Load the environment variable

    load_dotenv()

    # Config files paths (dates and assets come from the download config)
    download_configs_path = os.getenv("DOWNLOAD_CONFIGS_PATH")
    silver_configs_path = os.getenv("SILVER_CONFIGS_PATH")

    # Local bronze (output of the download) and silver roots
    bronze_root = os.getenv("BRONZE_ROOT")
    silver_root = os.getenv("SILVER_ROOT")

    # Execution logs path
    logs_path = os.getenv("LOGS_PATH")

    # Open the configs files

    with open(file = download_configs_path, mode = "r", encoding = "utf-8") as configs_download:

        download_params = json.load(configs_download)

    with open(file = silver_configs_path, mode = "r", encoding = "utf-8") as configs_silver:

        silver_params = json.load(configs_silver)

    # Class instance

    PDC = ProcessedDataCurator(
        start_date = download_params["start_date"],
        end_date = download_params["end_date"],
        assets = download_params["assets"],
        splits = silver_params["splits"],
        degraded_days = silver_params["degraded_days"],
        bronze_root = bronze_root,
        silver_root = silver_root,
        logs_path = logs_path
    )

    # Run the pipeline

    PDC.run()