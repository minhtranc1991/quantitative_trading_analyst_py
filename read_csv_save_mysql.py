import pandas as pd
import config 
from sqlalchemy import Table, Column, Integer, String, Float, MetaData, DateTime
from datetime import datetime

pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)

df = pd.read_csv('combined_ticker_data_usdt.csv')

df['open_time'] = pd.to_datetime(df['open_time'], unit='ms')
df['close_time'] = pd.to_datetime(df['close_time'], unit='ms')
#print(df.head())

# Load database configuration
_, db_config = config.load_config()
engine = config.create_database_engine()

tickers = df['ticker'].unique()

metadata = MetaData()

for ticker in tickers:
    # Định nghĩa bảng cho mỗi ticker
    table_name = ticker.lower().replace("usdt", "_usdt")
    print(f"Creating table: {table_name}")

    table = Table(
        table_name, metadata,
        Column('open_time', DateTime),
        Column('open', Float),
        Column('high', Float),
        Column('low', Float),
        Column('close', Float),
        Column('volume', Float),
        Column('close_time', DateTime),
        Column('quote_asset_volume', Float),
        Column('number_of_trades', Integer),
        Column('taker_buy_base_asset_volume', Float),
        Column('taker_buy_quote_asset_volume', Float),
        Column('ignore', Float),
        extend_existing=True
    )

    metadata.create_all(engine)

    ticker_data = df[df['ticker'] == ticker].drop(columns=['ticker'])
    
    ticker_data.to_sql(table_name, con=engine, if_exists='append', index=False)