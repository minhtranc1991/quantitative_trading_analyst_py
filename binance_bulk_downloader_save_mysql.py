import pandas as pd
import config 
from sqlalchemy import Table, Column, Integer, String, Float, MetaData, DateTime
from datetime import datetime
from binance_historical_data import BinanceDataDumper

# Load database configuration
_, db_config = config.load_config()
engine = config.create_database_engine()

pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)

data_dumper = BinanceDataDumper(
    path_dir_where_to_dump=".",
    asset_class="spot",  # spot, um, cm
    data_type="klines",  # aggTrades, klines, trades
    data_frequency="1d",
)

all_pairs = data_dumper.get_list_all_trading_pairs()
tickers = [pair for pair in all_pairs if pair.endswith("USDT")]

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

    # Định nghĩa bảng cho ticker với open_time làm khóa chính
    table = Table(
        table_name, metadata,
        Column('open_time', DateTime, primary_key=True),
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

    # Chèn dữ liệu vào bảng nếu không trùng lặp
    with engine.connect() as conn:
        for _, row in ticker_data.iterrows():
            # Kiểm tra xem open_time đã tồn tại chưa
            query = f"SELECT COUNT(*) FROM {table_name} WHERE open_time = :open_time"
            count = conn.execute(query, {'open_time': row['open_time']}).scalar()

            if count == 0:
                # Chèn dữ liệu nếu không tồn tại
                conn.execute(
                    table.insert().values(
                        open_time=row['open_time'],
                        open=row['open'],
                        high=row['high'],
                        low=row['low'],
                        close=row['close'],
                        volume=row['volume'],
                        close_time=row['close_time'],
                        quote_asset_volume=row['quote_asset_volume'],
                        number_of_trades=row['number_of_trades'],
                        taker_buy_base_asset_volume=row['taker_buy_base_asset_volume'],
                        taker_buy_quote_asset_volume=row['taker_buy_quote_asset_volume'],
                        ignore=row['ignore']
                    )
                )

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