import os
from dotenv import load_dotenv
import time
import hmac
from hashlib import sha256
import requests
import pandas as pd
from sqlalchemy import create_engine
from typing import Any, Dict, List

# --- Utility Functions ---
def test_mysql_connection(db_config: Dict[str, str]):
    """Test MySQL database connection."""
    try:
        engine = create_engine(
            f"mysql+pymysql://{db_config['user']}:{db_config['password']}@{db_config['host']}/{db_config['database']}"
        )
        with engine.connect() as connection:
            print("Connection to MySQL successful!")
    except Exception as e:
        print(f"Error connecting to MySQL: {e}")

def get_sign(api_secret: str, payload: str) -> str:
    """Generate HMAC SHA256 signature."""
    signature = hmac.new(api_secret.encode("utf-8"), payload.encode("utf-8"), digestmod=sha256).hexdigest()
    return signature

def send_request(method: str, path: str, query_params: str) -> Dict[str, Any]:
    """Send signed API request."""
    try:
        signature = get_sign(SECRET_KEY, query_params)
        url = f"{API_URL}{path}?{query_params}&signature={signature}"
        headers = {'X-BX-APIKEY': API_KEY}
        response = requests.request(method, url, headers=headers)
        response.raise_for_status()  # Raise error for non-2xx responses
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Request failed: {e}")
        return {"error": str(e)}

def fetch_market_data(symbols: List[str], interval: str = "1m", limit: int = 1440) -> Dict[str, pd.DataFrame]:
    """Fetch market data for multiple symbols."""
    end_time = int(time.time() * 1000)
    start_time = end_time - limit * 60 * 1000
    data = {}

    for symbol in symbols:
        query = f"symbol={symbol}&interval={interval}&limit={limit}&startTime={start_time}&endTime={end_time}"
        response = send_request("GET", "/openApi/swap/v3/quote/klines", query)

        if "data" in response and response["data"]:
            try:
                # Convert response data to DataFrame
                df = pd.DataFrame(response["data"], columns=["time", "open", "high", "low", "close", "volume"])
                df["time"] = pd.to_datetime(df["time"], unit="ms")  # Convert to datetime
                df[["open", "high", "low", "close", "volume"]] = df[["open", "high", "low", "close", "volume"]].astype(float)
                df["pnl_percentage"] = (df["close"] - df["open"]) / df["open"] * 100.0  # Calculate PnL percentage
                data[symbol] = df
            except Exception as e:
                print(f"Error processing data for {symbol}: {e}")
        else:
            print(f"No data returned for symbol {symbol}.")
    return data

def save_to_mysql(data: Dict[str, pd.DataFrame], db_config: Dict[str, str]):
    """Save market data to MySQL database."""
    try:
        engine = create_engine(
            f"mysql+pymysql://{db_config['user']}:{db_config['password']}@{db_config['host']}/{db_config['database']}"
        )
        with engine.connect() as connection:
            for symbol, df in data.items():
                table_name = symbol.replace("-", "_")  # Replace `-` with `_` to ensure valid table names
                df.to_sql(table_name, con=connection, if_exists="replace", index=False)
                print(f"Data for {symbol} saved to MySQL table {table_name}.")
    except Exception as e:
        print(f"Error saving data to MySQL: {e}")

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

        # Fetch and save data
        market_data = fetch_market_data(SYMBOLS)
        if market_data:
            save_to_mysql(market_data, DB_CONFIG)
