import os
from dotenv import load_dotenv, find_dotenv
import time
import hmac
from hashlib import sha256
import requests
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.sql import text

# --- Utility Functions ---
def test_mysql_connection(db_config):
    """Test MySQL database connection."""
    try:
        engine = create_engine(
            f"mysql+pymysql://{db_config['user']}:{db_config['password']}@{db_config['host']}/{db_config['database']}"
        )
        with engine.connect() as connection:
            print("Connection to MySQL successful!")
    except Exception as e:
        print(f"Error connecting to MySQL: {e}")

def get_sign(api_secret, payload):
    """Generate HMAC SHA256 signature."""
    signature = hmac.new(api_secret.encode("utf-8"), payload.encode("utf-8"), digestmod=sha256).hexdigest()
    return signature

def send_request(method, path, query_params):
    """Send signed API request."""
    try:
        signature = get_sign(SECRET_KEY, query_params)
        url = f"{API_URL}{path}?{query_params}&signature={signature}"
        headers = {'X-BX-APIKEY': API_KEY}
        response = requests.request(method, url, headers=headers)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Request failed: {e}")
        return {"error": str(e)}

def fetch_market_data(symbol, interval="1m", limit=1440):
    try:
        end_time = int(time.time() * 1000)
        start_time = end_time - limit * 60 * 1000
        query = f"symbol={symbol}&interval={interval}&limit={limit}&startTime={start_time}&endTime={end_time}"
        response = send_request("GET", "/openApi/swap/v3/quote/klines", query)
        #print(f"API response for {symbol}: {response}")

        if "data" in response and response["data"]:
            df = pd.DataFrame(response["data"], columns=["time", "open", "high", "low", "close", "volume"])
            df["time"] = pd.to_datetime(df["time"], unit="ms")
            df[["open", "high", "low", "close", "volume"]] = df[["open", "high", "low", "close", "volume"]].astype(float)
            #print(f"Fetched {df.shape[0]} rows for {symbol}.")
            return df
        else:
            print(f"No data returned for symbol {symbol}.")
    except Exception as e:
        print(f"Error fetching data for {symbol}: {e}")
    return pd.DataFrame()

def update_pnl(df):
    """Update PnL percentage using close prices of consecutive rows."""
    if not df.empty:
        # Loại bỏ dữ liệu trùng lặp
        df = df.drop_duplicates(subset=["time"], keep="last")
        # Sắp xếp lại theo thời gian
        df = df.sort_values(by="time")
        # Tính PnL dựa trên sự thay đổi của close
        df["pnl_percentage"] = df["close"].diff() / df["close"].shift(1) * 100
        # Xử lý giá trị NaN
        df["pnl_percentage"] = df["pnl_percentage"].fillna(0)
        #print(df)
    return df

from sqlalchemy import create_engine
from sqlalchemy.sql import text
import pandas as pd

from sqlalchemy import create_engine
from sqlalchemy.sql import text
import pandas as pd

def save_to_mysql(df, symbol, db_config):
    """
    Save DataFrame to a MySQL table. Handles table creation, duplicate removal, and row limit enforcement.

    Args:
        df (pd.DataFrame): DataFrame containing data to save. Must include a 'time' column in datetime format.
        symbol (str): Symbol name for the table (e.g., "BTC-USDT").
        db_config (dict): MySQL configuration (host, user, password, database).

    Returns:
        None
    """
    try:
        # Check if DataFrame is empty
        if df.empty:
            print(f"No data to save for {symbol}.")
            return

        # Ensure 'time' column is present and in datetime format
        if "time" not in df.columns:
            raise ValueError("DataFrame must contain a 'time' column.")
        if not pd.api.types.is_datetime64_any_dtype(df["time"]):
            df["time"] = pd.to_datetime(df["time"])
            print("Converted 'time' column to datetime.")

        # Create database engine
        engine = create_engine(
            f"mysql+pymysql://{db_config['user']}:{db_config['password']}@{db_config['host']}/{db_config['database']}"
        )
        table_name = symbol.replace("-", "_").lower()

        with engine.connect() as connection:
            # Step 1: Create table if it doesn't exist
            connection.execute(text(f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    time DATETIME PRIMARY KEY,
                    open FLOAT,
                    high FLOAT,
                    low FLOAT,
                    close FLOAT,
                    volume FLOAT,
                    pnl_percentage FLOAT
                )
            """))
            #print(f"Table {table_name} exists or created.")

            # Step 2: Fetch existing times to avoid duplicates
            existing_times_query = f"SELECT time FROM {table_name}"
            try:
                existing_times = pd.read_sql(existing_times_query, con=connection)
                existing_times["time"] = pd.to_datetime(existing_times["time"])
                #print(f"Existing times fetched: {existing_times.shape[0]} rows.")
                # Remove rows already present in the database
                df = df[~df["time"].isin(existing_times["time"])]
            except Exception as e:
                print(f"No existing times fetched for {table_name}: {e}")

            #print(f"New rows to insert: {df.shape[0]} rows.")

            # Step 3: Insert new rows if any
            if not df.empty:
                df.to_sql(table_name, con=connection, if_exists="append", index=False)
                #print(f"Inserted {df.shape[0]} new rows into {table_name}.")
            else:
                print(f"No new rows to insert for {symbol}.")

            # Step 4: Enforce row limit (max 1440 rows)
            row_count_query = f"SELECT COUNT(*) FROM {table_name}"
            result = connection.execute(text(row_count_query)).fetchone()
            row_count = result[0] if result else 0
            #print(f"Row count in {table_name}: {row_count}.")

            if row_count > 1440:
                delete_count = row_count - 1440
                connection.execute(text(f"""
                    DELETE FROM {table_name}
                    WHERE time IN (
                        SELECT time FROM (
                            SELECT time FROM {table_name} ORDER BY time ASC LIMIT {delete_count}
                        ) AS subquery
                    )
                """))
                print(f"Deleted {delete_count} oldest rows from {table_name}.")
    except Exception as e:
        print(f"Error saving data to MySQL for {symbol}: {e}")



# --- Main Execution ---
if __name__ == "__main__":
    # Load configuration from .env file
    load_dotenv(dotenv_path="D:/Python/quantitative_trading_analyst_py/LASSO-model/config.env")

    # --- Configuration ---
    API_URL = os.getenv("API_URL")
    API_KEY = os.getenv("API_KEY")
    SECRET_KEY = os.getenv("SECRET_KEY")

    DB_CONFIG = {
        "host": os.getenv("DB_HOST"),
        "user": os.getenv("DB_USER"),
        "password": os.getenv("DB_PASSWORD"),
        "database": os.getenv("DB_NAME"),
    }

    SYMBOLS = ["BTC-USDT", "ETH-USDT", "BNB-USDT", "XRP-USDT", "SOL-USDT"]

    if not all([API_URL, API_KEY, SECRET_KEY]):
        print("API configuration is missing!")
    elif not all(DB_CONFIG.values()):
        print("Database configuration is missing!")
    else:
        # Test database connection
        test_mysql_connection(DB_CONFIG)

        # Fetch and save data every minute
        while True:
            for symbol in SYMBOLS:
                df = fetch_market_data(symbol)
                if not df.empty:
                    df = update_pnl(df)
                    save_to_mysql(df, symbol, DB_CONFIG)
            print("Waiting for the next minute...")
            time.sleep((60 - time.localtime().tm_sec) % 60)