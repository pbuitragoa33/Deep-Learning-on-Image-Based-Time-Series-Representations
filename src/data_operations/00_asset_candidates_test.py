# 0. Candidate Screening


# This file aims to:

#   - Count how many 1-minute bars each candidate asset has per trading day in the dataset
#   - Do it without downloading data (metadata only), year by year
#   - Compare candidates against the assets already downloaded (SPY, XLE, VIXY)


# Libraries

import os
import pandas as pd
import pandas_market_calendars as mcal
import databento as db
from dotenv import load_dotenv


# Parameters

DATASET = "XNAS.ITCH"
SCHEMA = "ohlcv-1m"
START = "2018-05-01"
END = "2026-10-28"

# SPY, XLE, VIXY: reference assets (most liquid and much more data)
# USO, BNO, UNG, XOP, OIH: oil, gas, energy related
# TLT, IEF, UUP, HYG, GLD, EEM: gold, global growth, bonds

REFERENCE = ["SPY", "XLE", "VIXY"]
CANDIDATES = ["USO", "BNO", "UNG", "XOP", "OIH",  
              "TLT", "IEF", "UUP", "HYG",       
              "GLD", "EEM"]                        


# Function 1: year_windows

def year_windows(start, end):

    # One window per calendar year, trimmed to the start and end dates

    first_year = int(start[:4]) + 1
    last_year = int(end[:4])

    edges = [start] + [f"{year}-01-01" for year in range(first_year, last_year + 1)] + [end]

    return list(zip(edges[:-1], edges[1:]))


# Function 2: trading_days

def trading_days(start, end):

    # NYSE sessions in [start, end)

    nyse = mcal.get_calendar("NYSE")

    last_day = pd.Timestamp(end) - pd.Timedelta(days = 1)

    return len(nyse.schedule(start_date = start, end_date = last_day))



# Execution


if __name__ == "__main__":

    load_dotenv()

    client = db.Historical(os.getenv("DATABENTO_API_KEY"))

    rows = []

    for start, end in year_windows(START, END):

        days = trading_days(start, end)

        for symbol in REFERENCE + CANDIDATES:

            n = client.metadata.get_record_count(dataset = DATASET,
                                                 symbols = symbol,
                                                 schema = SCHEMA,
                                                 start = start,
                                                 end = end)

            rows.append({"symbol": symbol, "year": start[:4], "bars_per_day": round(n / days)})

        print(f"{start[:4]}: done ({days} trading days)")

    # Bars per trading day (includes pre and post market). The weakest year decides usability.

    table = pd.DataFrame(rows).pivot(index = "symbol", columns = "year", values = "bars_per_day")

    table["min"] = table.min(axis = 1)

    print(table.sort_values("min", ascending = False).to_string())