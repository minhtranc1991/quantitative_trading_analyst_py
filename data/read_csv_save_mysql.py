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

tikers_table = Table(
    'tikers', metadata,
    Column('table_name', String(255), primary_key=True),  # Đặt table_name là khóa chính để tránh trùng lặp
    extend_existing=True
)

# Tạo bảng tikers nếu chưa tồn tại
metadata.create_all(engine)

for ticker in tickers:
    # Xử lý tên bảng
    table_name = ticker.lower().replace("usdt", "_usdt")
    print(f"Creating table: {table_name}")  # Log tên bảng

    # Định nghĩa bảng cho ticker
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

    # Lọc dữ liệu theo ticker và loại bỏ cột ticker
    ticker_data = df[df['ticker'] == ticker].drop(columns=['ticker'])
    print(ticker_data)

    # Chèn dữ liệu vào bảng
    ticker_data.to_sql(table_name, con=engine, if_exists='append', index=False)

    # Kiểm tra và thêm table_name vào bảng tikers
    with engine.connect() as conn:
        # Truy vấn danh sách tên bảng hiện có trong tikers
        existing_tikers = conn.execute(tikers_table.select()).fetchall()
        existing_tikers = [row[0] for row in existing_tikers]

        if table_name not in existing_tikers:
            # Thêm table_name vào bảng tikers nếu chưa tồn tại
            conn.execute(tikers_table.insert().values(table_name=table_name))
            print(f"Added {table_name} to tikers")
        else:
            print(f"{table_name} already exists in tikers")