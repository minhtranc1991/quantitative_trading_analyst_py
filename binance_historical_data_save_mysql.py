import os
import time
import json
import config 
import pandas as pd
import urllib.request
import multiprocessing
from datetime import datetime, timedelta, date
from binance_historical_data import BinanceDataDumper
from sqlalchemy.orm import sessionmaker
from sqlalchemy import Table, Column, MetaData, DateTime, Float, Integer, String, PrimaryKeyConstraint, Date, inspect, update
from sqlalchemy.dialects.mysql import insert

# Khai báo metadata
metadata = MetaData()

# Định nghĩa bảng `tickers`
tickers_table = Table(
    'tickers', metadata,
    Column('ticker', String(50), primary_key=True),
    Column('first_open_time', Date, nullable=False),
    Column('last_updated_date', Date, nullable=False),
    Column('name', String(50), nullable=False)
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
    exclude_keywords = ["UPUSDT", "DOWNUSDT", "BEARUSDT", "BULLUSDT"]
    return [ticker for ticker in tickers if ticker.endswith("USDT") and not any(ex in ticker for ex in exclude_keywords)]

# Hàm để tạo tên bảng từ ticker
def get_table_name(ticker):
    return ticker.lower().replace("usdt", "_usdt")

# Hàm để kiểm tra và tạo bảng `tickers` nếu chưa tồn tại
def ensure_tickers_table_exists():
    inspector = inspect(engine)  # Sử dụng inspect để kiểm tra bảng
    if not inspector.has_table('tickers'):
        metadata.create_all(engine, [tickers_table])
        print("Bảng 'tickers' đã được tạo.")

def get_tickers_data():
    """
    Lấy toàn bộ dữ liệu `last_updated_date` và `first_open_time` từ bảng tickers dưới dạng dictionary.
    Nếu bảng không tồn tại, sẽ tự động khởi tạo bảng.
    Trả về:
    - dict dạng {ticker: {'last_updated_date': date, 'first_open_time': date}}
    """
    try:
        ensure_tickers_table_exists()

        result = session.query(
            tickers_table.c.ticker,
            tickers_table.c.last_updated_date,
            tickers_table.c.first_open_time,
            tickers_table.c.name
        ).all()

        tickers_data = {
            row.ticker: {
                'last_updated_date': row.last_updated_date,
                'first_open_time': row.first_open_time,
                'name': row.name
            }
            for row in result
        }

        return tickers_data

    except Exception as e:
        print(f"Lỗi khi lấy dữ liệu tickers: {str(e)}")
        return {}

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
    data_dict = data.to_dict(orient='records')
    if not data_dict:
        return
    
    table = Table(table_name, metadata, autoload_with=engine)
    statement = insert(table).prefix_with("IGNORE").values(data_dict)
    session.execute(statement)

# Hàm để cập nhật last_updated_date trong bảng tickers
def update_tickers_table(ticker, first_open_time, last_updated_date, name):
    stmt = insert(tickers_table).values(
        ticker=ticker,
        first_open_time=first_open_time,
        last_updated_date=last_updated_date,
        name=name
    )

    stmt = stmt.on_duplicate_key_update(
        first_open_time=stmt.inserted.first_open_time,
        last_updated_date=stmt.inserted.last_updated_date,
        name=stmt.inserted.name
    )

    session.execute(stmt)

# Hàm để lấy tất cả các file CSV từ một thư mục
def get_csv_files(directory):
    try:
        if not os.path.exists(directory):
            print(f"Warning: Thư mục không tồn tại: {directory}")
            return []  # Trả về danh sách rỗng thay vì raise error
        return [os.path.join(directory, f) for f in os.listdir(directory) if f.endswith('.csv')]
    except Exception as e:
        print(f"Lỗi khi đọc thư mục {directory}: {str(e)}")
        return []

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

def download_ticker(ticker, date_start):
    """Hàm thực hiện tải dữ liệu cho một ticker cụ thể"""
    data_dumper = BinanceDataDumper(
        path_dir_where_to_dump=".",
        asset_class="spot",
        data_type="klines",
        data_frequency="1h",
    )
    data_dumper.dump_data(
        tickers=ticker,
        date_start=date_start,
        date_end=date.today(),
        is_to_update_existing=False,
    )

def format_time(seconds):
    """Chuyển đổi giây thành định dạng hh:mm:ss."""
    hours = int(seconds // 3600)
    minutes = int((seconds % 3600) // 60)
    secs = int(seconds % 60)
    return f"{hours:02d}:{minutes:02d}:{secs:02d}"

# Hàm chính
def main():
    start_time = time.time()
    print("Bắt đầu xử lý tickers...")

    tickers = filter_usdt_tickers(get_list_all_trading_pairs())
    print(f"Lấy danh sách tickers: {len(tickers)} tickers, thời gian: {format_time(time.time() - start_time)}")

    tickers_data = get_tickers_data()
    print(f"Lấy dữ liệu tickers: thời gian: {format_time(time.time() - start_time)}")

    tickers_update_info = [] # Khởi tạo list để sort sau khi update

    total_tickers = len(tickers)
    for index, ticker in enumerate(tickers):
        ticker_start_time = time.time()

        table_name = get_table_name(ticker)
        data_info = tickers_data.get(ticker, {})

        if data_info:
            tickers_update_info.append({
                'ticker': ticker,
                'last_updated_date': data_info.get('last_updated_date'),
                'first_open_time': data_info.get('first_open_time'),
                'name': table_name
            })
            continue

        first_open_time = find_first_data_date(ticker)
        if first_open_time is None:
            print(f"Bỏ qua {ticker} do không tìm thấy dữ liệu đầu tiên.")
            continue

        last_updated_date = first_open_time

        try:
            update_tickers_table(ticker, first_open_time, last_updated_date, table_name)
            session.commit()
            tickers_update_info.append({
                'ticker': ticker,
                'last_updated_date': last_updated_date,
                'first_open_time': first_open_time,
                'name': table_name
            })
            print(f"{ticker}: {format_time(time.time() - ticker_start_time)}, {index + 1}/{total_tickers}")
        except Exception as e:
            session.rollback()
            print(f"Lỗi khi cập nhật {ticker}: {str(e)}, thời gian: {format_time(time.time() - ticker_start_time)}")
            continue

    # Sắp xếp ticker theo ngày cập nhật gần nhất
    tickers_update_info.sort(key=lambda x: x['last_updated_date'])
    tickers = [info['ticker'] for info in tickers_update_info]

    print(f"Sắp xếp tickers theo ngày cập nhật: thời gian: {format_time(time.time() - start_time)}")
    print(f"Hoàn thành xử lý và cập nhật tickers, tổng thời gian: {format_time(time.time() - start_time)}")
    
    for i, ticker in enumerate(tickers):
        ticker_start_time = time.time()

        print(f"🔄 Đang xử lý {ticker}... (Ticker {i + 1}/{len(tickers)})")

        ticker_info = next((info for info in tickers_update_info if info['ticker'] == ticker), {})
        date_start = ticker_info.get('last_updated_date')
        first_open_time = ticker_info.get('first_open_time')

        if not date_start:
            date_start = find_first_data_date(ticker)
        else:
            date_start = date_start + timedelta(days=-1)

        try:
            download_ticker(ticker, date_start)
            elapsed_time = time.time() - ticker_start_time

            time_stop = 90
            if elapsed_time > time_stop:
                print(f"⚠️ Bỏ qua {ticker}: Thời gian chạy vượt quá {time_stop} giây")
                continue

        except Exception as e:
            print(f"❌ Lỗi {ticker}: {e}")
        
        # Xử lý dữ liệu
        daily_files = os.path.join(os.getcwd(), f"spot/daily/klines/{ticker}/1h")
        monthly_files = os.path.join(os.getcwd(), f"spot/monthly/klines/{ticker}/1h")

        daily_files = get_csv_files(daily_files)
        monthly_files = get_csv_files(monthly_files)
        all_files = daily_files + monthly_files

        if not all_files:
            print(f"❗ Không có file CSV nào cho {ticker}")
            continue

        data = pd.concat([read_csv_file(file) for file in all_files], ignore_index=True)
        data.sort_values(by='open_time', inplace=True)

        table_name = ticker_info.get('name')
        create_table_if_not_exists(table_name)

        try:
            save_data_to_table(table_name, data)
            update_tickers_table(ticker, first_open_time, date.today() - timedelta(days=1), table_name)
            session.commit()
        except FileNotFoundError as e:
            print(f"❗ Lỗi không tìm thấy file cho {ticker}: {str(e)}")
            continue
        except Exception as e:
            print(f"❗ Lỗi nghiêm trọng khi xử lý {ticker}: {str(e)}")
            session.rollback()
            continue
        finally:
            session.close()

        # Hiển thị trạng thái và ước tính thời gian còn lại ở cuối vòng lặp
        elapsed_time = time.time() - ticker_start_time
        print(f"{ticker}: {format_time(elapsed_time)}, {i + 1}/{len(tickers)}", end="")

        avg_ticker_time = (time.time() - start_time) / (i + 1)
        remaining_tickers = len(tickers) - i - 1
        estimated_time = avg_ticker_time * remaining_tickers
        print(f", Ước tính còn lại: {format_time(estimated_time)}")

# Chạy hàm chính
if __name__ == "__main__":
    # Khởi tạo engine và session
    engine = config.create_database_engine()
    Session = sessionmaker(bind=engine)
    session = Session()

    main()
    
    session.close()