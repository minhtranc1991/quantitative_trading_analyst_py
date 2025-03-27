import os
import time
import config
import shutil
import pandas as pd
from datetime import datetime, date, timedelta
from sqlalchemy.orm import sessionmaker
from sqlalchemy import Table, Column, MetaData, DateTime, Float, Integer, String, Date, inspect, insert, select, text
import traceback
import concurrent.futures

# Khai báo metadata
metadata = MetaData()

tickers_table = Table(
    'tickers', metadata,
    Column('ticker', String(50), primary_key=True),
    Column('first_open_time', Date, nullable=False),
    Column('last_updated_date', Date, nullable=False)
)

# Hàm để kiểm tra và tạo bảng nếu chưa tồn tại
def create_table_if_not_exists(table_name):
    inspector = inspect(engine)
    if not inspector.has_table(table_name):
        table = Table(
            table_name, metadata,
            Column('open_time', DateTime, primary_key=True),
            Column('open', Float, nullable=False),
            Column('high', Float, nullable=False),
            Column('low', Float, nullable=False),
            Column('close', Float, nullable=False),
            Column('volume', Float),
            Column('close_time', DateTime),
            Column('quote_asset_volume', Float),
            Column('number_of_trades', Integer),
            Column('taker_buy_base_asset_volume', Float),
            Column('taker_buy_quote_asset_volume', Float),
            Column('ignore', Float)
        )
        metadata.create_all(engine)
        print(f"Bảng '{table_name}' đã được tạo.")

# Hàm để lưu dữ liệu vào bảng (batch insert)
def save_data_to_table(table_name, data, batch_size=1000):
    data.dropna(inplace=True)  # Loại bỏ các bản ghi chứa giá trị NaN
    data_dict = data.to_dict(orient='records')
    if not data_dict:
        print(f"Không có dữ liệu để lưu vào bảng '{table_name}'.")
        return

    table = Table(table_name, metadata, autoload_with=engine)
    
    try:
        with engine.begin() as connection:
            for i in range(0, len(data_dict), batch_size):
                batch = data_dict[i:i + batch_size]
                connection.execute(insert(table).prefix_with("IGNORE"), batch)
        print(f"Đã lưu {len(data_dict)} bản ghi vào bảng '{table_name}'.")
    except Exception as e:
        print(f"Lỗi khi lưu dữ liệu vào bảng '{table_name}': {str(e)}")

# Hàm để cập nhật last_updated_date và first_open_time trong bảng tickers
def update_ticker_table(ticker, first_open_time, last_updated_date):
    ticker_exists = session.query(
        session.query(tickers_table).filter_by(ticker=ticker).exists()
    ).scalar()

    if ticker_exists:
        update_statement = tickers_table.update().where(
            tickers_table.c.ticker == ticker
        ).values(
            first_open_time=first_open_time,
            last_updated_date=last_updated_date
        )
        print(f"Cập nhật bảng tickers cho '{ticker}': first_open_time={first_open_time}, last_updated_date={last_updated_date}")
    else:
        update_statement = tickers_table.insert().values(
            ticker=ticker,
            first_open_time=first_open_time,
            last_updated_date=last_updated_date
        )
        print(f"Thêm mới bản ghi vào bảng tickers cho '{ticker}': first_open_time={first_open_time}, last_updated_date={last_updated_date}")

    session.execute(update_statement)

# Hàm để lấy tất cả các file CSV từ một thư mục
def get_csv_files(directory):
    try:
        if not os.path.exists(directory):
            print(f"Warning: Thư mục không tồn tại: {directory}")
            return []
        return [os.path.join(directory, f) for f in os.listdir(directory) if f.endswith('.csv')]
    except Exception as e:
        print(f"Lỗi khi đọc thư mục {directory}: {str(e)}\n{traceback.format_exc()}")
        return []

# Hàm để xác định đơn vị của timestamp
def detect_timestamp_unit(timestamp):
    num_digits = len(str(timestamp))
    if num_digits == 13:
        return 'ms'
    elif num_digits == 16:
        return 'us'
    else:
        raise ValueError(f"Timestamp không hợp lệ: {timestamp}")

# Hàm để chuyển đổi timestamp sang datetime
def convert_timestamp(timestamp):
    unit = detect_timestamp_unit(timestamp)
    return pd.to_datetime(timestamp, unit=unit, errors='coerce')

def read_csv_file(file_path):
    try:
        if not os.path.exists(file_path):
            print(f"File not found: {file_path}")
            return pd.DataFrame()

        df = pd.read_csv(file_path)
        df.columns = [
            "open_time", "open", "high", "low", "close", "volume", "close_time",
            "quote_asset_volume", "number_of_trades", "taker_buy_base_asset_volume",
            "taker_buy_quote_asset_volume", "ignore"
        ]

        # Xử lý các trường null
        df = df.dropna()

        # Xử lý các trường trùng lặp
        df = df.drop_duplicates()

        # Chuyển đổi timestamp sang datetime
        df['open_time'] = df['open_time'].apply(convert_timestamp)
        df['close_time'] = df['close_time'].apply(convert_timestamp)

        # Kiểm tra dữ liệu hợp lệ
        if df.empty or not all(col in df.columns for col in ["open_time", "open", "high", "low", "close"]):
            print(f"Dữ liệu không hợp lệ từ file {file_path}")
            return pd.DataFrame()

        # print(f"Đọc thành công file CSV: {file_path}, số bản ghi: {len(df)}")
        return df
    except Exception as e:
        print(f"Lỗi khi đọc file CSV {file_path}: {str(e)}\n{traceback.format_exc()}")
        return pd.DataFrame()

# Hàm để tạo tên bảng từ ticker
def get_table_name(ticker):
    return ticker.lower().replace("usdt", "_usdt")

def process_ticker(ticker, last_updated_date=None):
    table_name = get_table_name(ticker)
    daily_files = os.path.join(os.getcwd(), f"binance_data/spot//{ticker}/1h")
    all_files = get_csv_files(daily_files)

    if not all_files:
        print(f"Không có file CSV nào cho {ticker}")
        return

    data = pd.concat([read_csv_file(file) for file in all_files], ignore_index=True)
    if data.empty:
        print(f"Không có dữ liệu hợp lệ cho {ticker}")
        return

    data.sort_values(by='open_time', inplace=True)

    if last_updated_date:
        data = data[data['open_time'] >= pd.to_datetime(last_updated_date)]

    if data.empty:
        print(f"Không có dữ liệu mới cho {ticker} sau ngày {last_updated_date}")
        return

    create_table_if_not_exists(table_name)

    try:
        save_data_to_table(table_name, data)
        first_open_time = data['open_time'].min().date()
        update_ticker_table(table_name, first_open_time, date.today())
        session.commit()
        print(f"Đã xử lý thành công ticker: {ticker}")
    except Exception as e:
        print(f"Lỗi nghiêm trọng khi xử lý {ticker}: {str(e)}\n{traceback.format_exc()}")
        session.rollback()

def delete_unwanted_folders(folder_path="binance_data/spot/"):
    """Xóa các thư mục có chứa UPUSDT, DOWNUSDT, BEARUSDT, BULLUSDT."""
    full_path = os.path.join(os.getcwd(), folder_path)

    if not os.path.exists(full_path):
        print(f"Thư mục không tồn tại: {full_path}")
        return

    try:
        for item in os.listdir(full_path):
            item_path = os.path.join(full_path, item)
            if os.path.isdir(item_path) and any(keyword in item for keyword in ["UPUSDT", "DOWNUSDT", "BEARUSDT", "BULLUSDT"]):
                shutil.rmtree(item_path)
                print(f"Đã xóa thư mục: {item_path}")
    except Exception as e:
        print(f"Lỗi khi xóa thư mục trong {full_path}: {str(e)}")

def get_tickers_from_folder(folder_path="binance_data/spot/"):
    """Lấy danh sách các tickers từ thư mục chỉ định."""
    tickers = []
    full_path = os.path.join(os.getcwd(), folder_path)

    delete_unwanted_folders(folder_path)

    if not os.path.exists(full_path):
        print(f"Thư mục không tồn tại: {full_path}")
        return tickers

    try:
        for item in os.listdir(full_path):
            item_path = os.path.join(full_path, item)
            if os.path.isdir(item_path):
                tickers.append(item)
    except Exception as e:
        print(f"Lỗi khi đọc thư mục {full_path}: {str(e)}")

    return tickers

def format_time(seconds):
    return str(timedelta(seconds=int(seconds)))

def get_last_updated_dates():
    try:
        result = session.execute(select(tickers_table.c.ticker, tickers_table.c.last_updated_date)).fetchall()
        return {row.ticker: row.last_updated_date for row in result}
    except Exception as e:
        print(f"Lỗi khi lấy danh sách last_updated_date: {str(e)}")
        return {}

# Hàm chính
def main():
    tickers = get_tickers_from_folder()
    last_updated_dates = get_last_updated_dates()

    total_tickers = len(tickers)
    processed_tickers = 0
    start_time = time.time()

    for ticker in tickers:
        last_updated_date = last_updated_dates.get(get_table_name(ticker))
        process_ticker(ticker, last_updated_date)
        processed_tickers += 1
        
        elapsed_time = time.time() - start_time
        avg_time_per_ticker = elapsed_time / processed_tickers if processed_tickers > 0 else 0
        remaining_tickers = total_tickers - processed_tickers
        estimated_remaining_time = avg_time_per_ticker * remaining_tickers

        print(f"Đã xử lý {processed_tickers}/{total_tickers} tickers. "
              f"Thời gian đã trôi qua: {format_time(elapsed_time)}. "
              f"Ước tính thời gian còn lại: {format_time(estimated_remaining_time)}.")

if __name__ == "__main__":
    engine = config.create_database_engine()
    Session = sessionmaker(bind=engine)
    session = Session()

    main()

    session.close()