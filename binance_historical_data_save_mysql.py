import os
import time
import json
import pandas as pd
import urllib.request
import config 
from datetime import datetime, timedelta, date
from sqlalchemy import Table, Column, MetaData, DateTime, Float, Integer, String, PrimaryKeyConstraint, Date, inspect
from sqlalchemy.orm import sessionmaker
from binance_historical_data import BinanceDataDumper

# Khởi tạo engine và session
engine = config.create_database_engine()
Session = sessionmaker(bind=engine)
session = Session()

# Khai báo metadata
metadata = MetaData()

# Định nghĩa lại bảng `tickers` theo yêu cầu của bạn
tickers_table = Table(
    'tickers', metadata,
    Column('ticker', String(50), primary_key=True),  # ticker CHAR PRIMARY KEY
    Column('last_updated_date', Date, nullable=False)  # last_updated_date date NOT NULL
)

# Hàm để lấy danh sách các cặp giao dịch từ Binance
def get_list_all_trading_pairs():
    # Sử dụng BinanceDataDumper để lấy danh sách các cặp giao dịch
    data_dumper = BinanceDataDumper(
        path_dir_where_to_dump=".",
        asset_class="spot",  # Lấy các cặp giao dịch spot
        data_type="klines",  # Loại dữ liệu (không quan trọng ở đây)
        data_frequency="1h",  # Tần suất dữ liệu (không quan trọng ở đây)
    )
    # Lấy danh sách các cặp giao dịch
    trading_pairs = data_dumper.get_list_all_trading_pairs()
    return trading_pairs

# Hàm để lọc các tickers có đuôi USDT
def filter_usdt_tickers(tickers):
    return [ticker for ticker in tickers if ticker.endswith("USDT")]

# Hàm để tạo tên bảng từ ticker
def get_table_name(ticker):
    return ticker.lower().replace("usdt", "_usdt")

# Hàm để kiểm tra và tạo bảng `tickers` nếu chưa tồn tại
def ensure_tickers_table_exists():
    inspector = inspect(engine)  # Sử dụng inspect để kiểm tra bảng
    if not inspector.has_table('tickers'):
        metadata.create_all(engine, [tickers_table])
        print("Bảng 'tickers' đã được tạo.")

# Hàm để kiểm tra và lấy ngày cuối cùng được cập nhật
def get_last_updated_date(ticker):
    # Đảm bảo bảng `tickers` tồn tại
    ensure_tickers_table_exists()
    
    # Truy vấn ngày cuối cùng được cập nhật
    result = session.query(tickers_table).filter(tickers_table.c.ticker == ticker).first()
    return result.last_updated_date if result else None

# Hàm để tìm ngày đầu tiên có dữ liệu
def find_first_data_date(ticker):
    # Sử dụng BinanceDataDumper để lấy ngày đầu tiên có dữ liệu
    data_dumper = BinanceDataDumper(
        path_dir_where_to_dump=".",
        asset_class="spot",  # Lấy dữ liệu spot
        data_type="klines",  # Loại dữ liệu
        data_frequency="1h",  # Tần suất dữ liệu
    )
    # Lấy ngày đầu tiên có dữ liệu
    min_start_date = data_dumper.get_min_start_date_for_ticker(ticker)
    return min_start_date

# Hàm để tạo bảng nếu chưa tồn tại
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

# Hàm để lưu dữ liệu vào bảng
def save_data_to_table(table_name, data):
    # Chuyển đổi DataFrame thành danh sách các từ điển
    data_dict = data.to_dict(orient='records')
    
    # Lấy đối tượng bảng
    table = Table(table_name, metadata, autoload_with=engine)
    
    # Chèn dữ liệu vào bảng
    session.execute(table.insert(), data_dict)
    session.commit()

# Hàm để cập nhật last_updated_date trong bảng tickers
def update_last_updated_date(ticker, last_updated_date):
    # Tạo một đối tượng mới hoặc cập nhật đối tượng hiện có
    ticker_record = session.query(tickers_table).filter(tickers_table.c.ticker == ticker).first()
    
    if ticker_record:
        # Nếu ticker đã tồn tại, cập nhật last_updated_date
        ticker_record.last_updated_date = last_updated_date
    else:
        # Nếu ticker chưa tồn tại, chèn dữ liệu mới
        new_record = tickers_table.insert().values(ticker=ticker, last_updated_date=last_updated_date)
        session.execute(new_record)
    
    session.commit()

# Hàm để lấy tất cả các file CSV từ một thư mục
def get_csv_files(directory):
    if not os.path.exists(directory):
        raise FileNotFoundError(f"Thư mục không tồn tại: {directory}")
    return [os.path.join(directory, f) for f in os.listdir(directory) if f.endswith('.csv')]

# Hàm để xác định đơn vị của timestamp
def detect_timestamp_unit(timestamp):
    # Đếm số chữ số của timestamp
    num_digits = len(str(timestamp))
    
    if num_digits == 13:
        return 'ms'  # Milli giây
    elif num_digits == 16:
        return 'us'  # Micro giây
    else:
        raise ValueError(f"Timestamp không hợp lệ: {timestamp}")
    
# Hàm để chuyển đổi timestamp sang datetime
def convert_timestamp(timestamp):
    unit = detect_timestamp_unit(timestamp)
    return pd.to_datetime(timestamp, unit=unit, errors='coerce')

def read_csv_file(file_path):
    # Đọc file CSV
    df = pd.read_csv(file_path)
    
    df.columns = [
        "open_time",  
        "open",     
        "high",       
        "low",     
        "close",    
        "volume", 
        "close_time", 
        "quote_asset_volume",      
        "number_of_trades",        
        "taker_buy_base_asset_volume",  
        "taker_buy_quote_asset_volume", 
        "ignore"                     
        ]
    # Chuyển đổi open_time và close_time sang kiểu datetime
    df['open_time'] = df['open_time'].apply(convert_timestamp)
    df['close_time'] = df['close_time'].apply(convert_timestamp)
    
    return df

# Hàm chính
def main():
    # Lấy danh sách tất cả các tickers
    tickers = get_list_all_trading_pairs()
    
    # Lọc các tickers có đuôi USDT
    tickers = filter_usdt_tickers(tickers)
    
    # Duyệt qua từng ticker
    for ticker in tickers:
        table_name = get_table_name(ticker)
        
        # Kiểm tra ngày cuối cùng được cập nhật
        date_start = get_last_updated_date(ticker)
        
        # Nếu ticker chưa có trong bảng tickers, tìm ngày đầu tiên có dữ liệu
        if not date_start:
            date_start = find_first_data_date(ticker)
        else:
            date_start = date_start + timedelta(days=-1)
        
        # Tải dữ liệu từ Binance
        data_dumper = BinanceDataDumper(
            path_dir_where_to_dump=".",
            asset_class="spot",
            data_type="klines",
            data_frequency="1h",
        )
        
        date_end = date.today()

        # Gọi hàm dump_data với các tham số đã chuyển đổi
        data_dumper.dump_data(
            tickers=ticker,
            date_start=date_start,
            date_end=date_end,
            is_to_update_existing=False,
        )
        
        # Đọc dữ liệu từ file
        # Đường dẫn đến hai thư mục chứa dữ liệu
        daily_files = os.path.join(os.getcwd(), f"spot/daily/klines/{ticker}/1h")
        monthly_files = os.path.join(os.getcwd(), f"spot/monthly/klines/{ticker}/1h")

        # Lấy tất cả các file CSV từ cả hai thư mục
        daily_files = get_csv_files(daily_files)
        monthly_files = get_csv_files(monthly_files)

        # Kết hợp và sắp xếp danh sách các file
        all_files = daily_files + monthly_files

        # Đọc và kết hợp dữ liệu từ tất cả các file
        data = pd.concat([read_csv_file(file) for file in all_files], ignore_index=True)
        
        # Sắp xếp dữ liệu theo open_time
        data.sort_values(by='open_time', inplace=True)
        #print(data.head())

        # Tạo bảng nếu chưa tồn tại
        create_table_if_not_exists(table_name)
        
        # Lưu dữ liệu vào bảng
        save_data_to_table(table_name, data)
        
        # Cập nhật last_updated_date trong bảng tickers
        update_last_updated_date(ticker, datetime.now())

# Chạy hàm chính
if __name__ == "__main__":
    main()
    session.close()