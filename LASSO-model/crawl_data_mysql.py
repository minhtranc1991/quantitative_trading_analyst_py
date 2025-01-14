import os
import time
import hmac
from hashlib import sha256
import requests
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv

# --- Configuration Management ---
def load_config(config_path):
    """Load API and database configuration from .env file."""
    load_dotenv(dotenv_path=config_path)
    api_config = {
        "url": os.getenv("API_URL"),
        "key": os.getenv("API_KEY"),
        "secret": os.getenv("SECRET_KEY"),
    }
    db_config = {
        "host": os.getenv("DB_HOST"),
        "user": os.getenv("DB_USER"),
        "password": os.getenv("DB_PASSWORD"),
        "database": os.getenv("DB_NAME"),
    }
    return api_config, db_config

# --- Database Management ---
def create_database_engine(db_config):
    """Create a database engine for MySQL."""
    return create_engine(
        f"mysql+pymysql://{db_config['user']}:{db_config['password']}@{db_config['host']}/{db_config['database']}"
    )

def test_database_connection(engine):
    """Test database connection, create a test table, and log verification."""
    try:
        with engine.connect() as connection:
            # Tạo bảng kiểm tra
            connection.execute(text("""
                CREATE TABLE IF NOT EXISTS test_connection (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    test_message VARCHAR(255),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """))
            print("Test table created successfully.")

            # Thêm dữ liệu vào bảng kiểm tra
            connection.execute(text("""
                INSERT INTO test_connection (test_message)
                VALUES ('Database connection test successful.')
            """))
            print("Inserted test data successfully.")

            # Đọc dữ liệu từ bảng kiểm tra
            result = connection.execute(text("SELECT * FROM test_connection"))
            rows = result.fetchall()
            if rows:
                print("Read test data successfully. Log:")
                for row in rows:
                    print(row)
            else:
                print("No test data found.")

            # Xóa bảng kiểm tra
            connection.execute(text("DROP TABLE test_connection"))
            print("Test table dropped successfully. Database connection verified!")
    except Exception as e:
        print(f"Database connection test failed: {e}")

# --- API Management ---
def generate_signature(api_secret, payload):
    """Generate HMAC SHA256 signature."""
    return hmac.new(api_secret.encode("utf-8"), payload.encode("utf-8"), digestmod=sha256).hexdigest()

def send_api_request(api_config, method, path, query_params):
    """Send a signed API request."""
    try:
        signature = generate_signature(api_config["secret"], query_params)
        url = f"{api_config['url']}{path}?{query_params}&signature={signature}"
        headers = {'X-BX-APIKEY': api_config['key']}
        response = requests.request(method, url, headers=headers)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"API request failed: {e}")
        return {"error": str(e)}

# --- Data Processing ---
def process_market_data(response_data):
    """Process raw market data into a DataFrame."""
    if "data" in response_data and response_data["data"]:
        df = pd.DataFrame(response_data["data"], columns=["time", "open", "high", "low", "close", "volume"])
        df["time"] = pd.to_datetime(df["time"], unit="ms")
        df[["open", "high", "low", "close", "volume"]] = df[["open", "high", "low", "close", "volume"]].astype(float)
        return df
    else:
        print("No market data returned.")
        return pd.DataFrame()

def calculate_pnl(df):
    """Calculate PnL percentage."""
    if not df.empty:
        df = df.drop_duplicates(subset=["time"]).sort_values(by="time")
        df["pnl_percentage"] = df["close"].pct_change() * 100
        df["pnl_percentage"] = df["pnl_percentage"].fillna(0)
    return df

def save_dataframe_to_mysql(df, symbol, session):
    """Save DataFrame to MySQL, managing duplicates and row limits."""
    table_name = symbol.replace("-", "_").lower()
    if df.empty:
        print(f"No data to save for {symbol}.")
        return

    try:
        # Create table if not exists
        session.execute(text(f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                time DATETIME PRIMARY KEY,
                open FLOAT, high FLOAT, low FLOAT, close FLOAT,
                volume FLOAT, pnl_percentage FLOAT
            )
        """))
        session.commit()

        # Get existing timestamps
        existing_times = session.execute(text(f"SELECT time FROM {table_name}")).fetchall()
        existing_times = {row[0] for row in existing_times}

        # Filter new data
        df = df[~df["time"].isin(existing_times)]

        if not df.empty:
            #print(df.head())
            #print(df.dtypes)
            df.to_sql(table_name, con=session.bind, if_exists="append", index=False)
            print(f"Data inserted successfully into table '{table_name}' at {datetime.now()}.")

        # Enforce row limit
        row_count = session.execute(text(f"SELECT COUNT(*) FROM {table_name}")).scalar()
        if row_count > 1440:
            delete_count = row_count - 1440
            session.execute(text(f"""
                DELETE FROM {table_name}
                WHERE time IN (
                    SELECT time FROM (
                        SELECT time FROM {table_name} ORDER BY time ASC LIMIT {delete_count}
                    ) AS subquery
                )
            """))
            session.commit()
            print(f"Deleted {delete_count} oldest rows from {table_name}.")

    except Exception as e:
        session.rollback()
        print(f"Error inserting data into table '{table_name}': {e}")

# --- Main Execution ---
def main(symbols):
    config_path = "D:/Python/quantitative_trading_analyst_py/LASSO-model/config.env"
    api_config, db_config = load_config(config_path)

    if not all(api_config.values()) or not all(db_config.values()):
        print("Configuration is incomplete!")
        return

    engine = create_database_engine(db_config)
    #test_database_connection(engine)
    Session = sessionmaker(bind=engine)

    while True:
        with Session() as session:
            for symbol in symbols:
                limit = 1440
                end_time = int(time.time() * 1000)
                start_time = end_time - limit * 60 * 1000  # 1440 minutes
                query = f"symbol={symbol}&interval=1m&limit={limit}&startTime={start_time}&endTime={end_time}"
                response = send_api_request(api_config, "GET", "/openApi/swap/v3/quote/klines", query)
                df = process_market_data(response)
                if not df.empty:
                    df = calculate_pnl(df)
                    save_dataframe_to_mysql(df, symbol, session)
        print("Waiting for the next minute...")
        time.sleep((60 - time.localtime().tm_sec) % 60)

if __name__ == "__main__":

    symbols = ["BTC-USDT", "ETH-USDT", "BNB-USDT", "XRP-USDT", "SOL-USDT"]
    main(symbols)