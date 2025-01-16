import time
import hmac
import requests
import pandas as pd
import config
from hashlib import sha256
from datetime import datetime
from sqlalchemy import text
from sqlalchemy.orm import sessionmaker

APIURL = "https://open-api.bingx.com"
APIKEY = ""
SECRETKEY = ""

# --- API Management ---
def get_sign(api_secret, payload):
    """Generate HMAC SHA256 signature."""
    return hmac.new(api_secret.encode("utf-8"), payload.encode("utf-8"), digestmod=sha256).hexdigest()

def send_api_request(method, path, urlpa, payload=None):
    url = "%s%s?%s&signature=%s" % (APIURL, path, urlpa, get_sign(SECRETKEY, urlpa))
    headers = {
        'X-BX-APIKEY': APIKEY,
    }
    response = requests.request(method, url, headers=headers, data=payload)
    return response.json()
    
def parseParam(paramsMap):
    sortedKeys = sorted(paramsMap)
    paramsStr = "&".join(["%s=%s" % (x, paramsMap[x]) for x in sortedKeys])
    if paramsStr != "": 
     return paramsStr+"&timestamp="+str(int(time.time() * 1000))
    else:
     return paramsStr+"timestamp="+str(int(time.time() * 1000))

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
    engine = config.create_database_engine()
    #test_database_connection(engine)
    Session = sessionmaker(bind=engine)

    while True:
        with Session() as session:
            for symbol in symbols:
                payload = {}
                path = '/openApi/swap/v3/quote/klines'
                method = "GET"
                params_map = {
                    "symbol": symbol,
                    "interval": "1m",
                    "limit": "1440"
                    }
                params_str = parseParam(params_map)
                response = send_api_request(method, path, params_str, payload)
                df = process_market_data(response)
                if not df.empty:
                    df = calculate_pnl(df)
                    save_dataframe_to_mysql(df, symbol, session)
        print("Waiting for the next minute...")
        time.sleep(max(0, 60 - (time.time() % 60)))

if __name__ == "__main__":

    symbols = ["BTC-USDT", "ETH-USDT", "BNB-USDT", "XRP-USDT", "SOL-USDT"]
    main(symbols)