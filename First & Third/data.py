import requests
import pandas as pd
from datetime import datetime, timedelta
import pytz
import pandas_ta as ta
from datetime import datetime, timedelta
from celery import Celery
from fake_useragent import UserAgent

# Configure Celery
app = Celery('bybit_tasks',
             broker='redis://:maisha123@localhost:6379/0',
             backend='redis://:maisha123@localhost:6379/0')

# Optional Celery configuration
app.conf.update(
    task_serializer='json',
    accept_content=['json'],
    result_serializer='json',
    timezone='Asia/Kolkata',
    enable_utc=True,
)


def get_bybit_data(symbol, category, interval, start_date, end_date):
    """
    Fetch historical kline (candlestick) data from Bybit API and compute indicators.
    """
    try:
        # Convert date to UTC timestamp in milliseconds
        start_ts = to_milliseconds(start_date, tz_str="Asia/Kolkata")
        end_ts = to_milliseconds(end_date, tz_str="Asia/Kolkata")
        ua = UserAgent()
        headers = {
            "User-Agent": ua.random  # sets a random user-agent
        }
        params = {
            "category": category,
            "symbol": symbol,
            "interval": interval,
            "start": start_ts,
            "end": end_ts,
            "limit": 960
        }

        # Make GET request to Bybit API
        response = requests.get(
            "https://api.bybit.com/v5/market/kline",
            params=params,
            headers=headers
        )
        response.raise_for_status()
        result = response.json()

        # Handle API errors or empty result
        if result["retCode"] != 0 or not result["result"]["list"]:
            print(f"No data or error: {result}")
            return pd.DataFrame()

        # Extract and format kline data
        klines = result["result"]["list"]
        df = pd.DataFrame(klines, columns=[
            "UTC_timestamp", "open", "high", "low", "close", "volume", "turnover"
        ])

        df["UTC_timestamp"] = pd.to_datetime(df["UTC_timestamp"].astype(int), unit="ms", utc=True)
        df["datetime_ist"] = df["UTC_timestamp"].dt.tz_convert("Asia/Kolkata")
        df[["open", "high", "low", "close", "volume"]] = df[["open", "high", "low", "close", "volume"]].astype(float)
        df["IST_timestamp"] = df["datetime_ist"].dt.tz_localize(None)
        df.set_index("IST_timestamp", inplace=True)
        df = df[~df.index.duplicated(keep="first")]
        df.sort_index(inplace=True)

        # Add metadata
        df["symbol"] = symbol
        df["category"] = category
        df["interval"] = interval

        # Add technical indicators
        df["VWAP"] = ta.vwap(df["high"], df["low"], df["close"], df["volume"])
        macd = ta.macd(df["close"])
        df = pd.concat([df, macd], axis=1)

        print(f"Records fetched: {len(df)}")
        return df

    except Exception as e:
        print("Error in get_bybit_data:", e)
        return pd.DataFrame()


def to_milliseconds(dt_str, tz_str="UTC"):
    """
    Convert a datetime string to UTC timestamp in milliseconds.
    Example input: "2021-01-01 12:30"
    """
    try:
        dt = datetime.strptime(dt_str, "%Y-%m-%d %H:%M")
        local = pytz.timezone(tz_str)
        localized_dt = local.localize(dt)
        utc_dt = localized_dt.astimezone(pytz.UTC)
        return int(utc_dt.timestamp() * 1000)
    except Exception as e:
        print("Error in to_milliseconds:", e)
        return 0


def generate_date_ranges(start_date, end_date, days_in_range=10):
    """
    Generate a list of date ranges split by a specified number of days.
    """
    try:
        start_dt = datetime.strptime(start_date, "%Y-%m-%d %H:%M")
        end_dt = datetime.strptime(end_date, "%Y-%m-%d %H:%M")
        date_ranges = []

        while start_dt < end_dt:
            range_end = start_dt + timedelta(days=days_in_range) - timedelta(minutes=1)
            if range_end > end_dt:
                range_end = end_dt

            date_ranges.append([start_dt.strftime("%Y-%m-%d %H:%M"), range_end.strftime("%Y-%m-%d %H:%M")])
            start_dt = range_end + timedelta(minutes=1)

        return date_ranges

    except Exception as e:
        print("Error in generate_date_ranges:", e)
        return []


@app.task(name="bybit_tasks.extract_data_task")
def extract_data(start_date, end_date, symbol, category, interval):
    """
    Celery task to extract data over date ranges and save to CSV.
    """
    try:
        date_ranges = generate_date_ranges(start_date, end_date)
        all_dataframes = []

        for i in date_ranges:
            df = get_bybit_data(symbol, category, interval, i[0], i[1])
            if not df.empty:
                all_dataframes.append(df)

        if len(all_dataframes) > 1:
            final_df = pd.concat(all_dataframes).sort_index()
            final_df = final_df[~final_df.index.duplicated(keep="first")]

            # Save final DataFrame to CSV
            file_name = f"""{symbol}_{category}_{interval}_{start_date.split(" ")[0]}_{end_date.split(" ")[0]}.csv"""
            final_df.to_csv(file_name)

            print(f"Total records in final DataFrame: {len(final_df)}")
        else:
            print("NO DATA FOUND....................")

        return {"message": "Please wait for a few seconds while it downloads your data."}

    except Exception as e:
        print("Error in extract_data task:", e)
        return {"message": "An error occurred while extracting data."}


def run_task(start_date, end_date, symbol, category):
    """
    Function to trigger the Celery task asynchronously.
    """
    try:
        id = extract_data.delay(start_date, end_date, symbol, category, interval=15)
        return {"message": f"Please wait for a few seconds while it downloads your data. TaskId : {id}"}
    except Exception as e:
        print("Error in run_task:", e)
        return {"message": "Failed to start the task."}
