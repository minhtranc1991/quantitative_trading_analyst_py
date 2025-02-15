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
from sqlalchemy import Table, Column, MetaData, DateTime, Float, Integer, String, PrimaryKeyConstraint, Date, inspect, insert


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
    data_dict = data.to_dict(orient='records')
    if not data_dict:
        return
    
    table = Table(table_name, metadata, autoload_with=engine)
    statement = insert(table).prefix_with("IGNORE").values(data_dict)
    session.execute(statement)

# Hàm để cập nhật last_updated_date trong bảng tickers
def update_last_updated_date(ticker, last_updated_date):
    ticker_exists = session.query(
        session.query(tickers_table).filter_by(ticker=ticker).exists()
    ).scalar()

    if ticker_exists:
        update_statement = tickers_table.update().where(
            tickers_table.c.ticker == ticker
        ).values(
            last_updated_date=last_updated_date
        )
    else:
        update_statement = tickers_table.insert().values(
            ticker=ticker,
            last_updated_date=last_updated_date
        )
    
    session.execute(update_statement)

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

# Hàm chính
def main():
    # Lấy danh sách tất cả các tickers
    tickers = get_list_all_trading_pairs()
    
    # Lọc các tickers có đuôi USDT
    tickers = filter_usdt_tickers(tickers)
    
    tickers_update_info = []

    for ticker in tickers:
        table_name = get_table_name(ticker)
        date_start = get_last_updated_date(table_name)
        
        # Nếu ticker chưa có trong bảng tickers, tìm ngày đầu tiên có dữ liệu
        if not date_start:
            date_start = find_first_data_date(ticker)
        else:
            date_start = date_start + timedelta(days=-1)

        try:
            update_last_updated_date(table_name, date_start)
            session.commit()  # Commit duy nhất
        except Exception as e:
            session.rollback()  # Rollback tất cả thay đổi nếu có lỗi
            print(f"Error occurred: {str(e)}")
            continue
        finally:
            session.close()

        # Lưu thông tin cập nhật của ticker vào danh sách
        tickers_update_info.append({
            'ticker': ticker,
            'last_updated_date': date_start
        })

    tickers_update_info.sort(key=lambda x: x['last_updated_date'])
    tickers = [info['ticker'] for info in tickers_update_info]
    
    # Duyệt qua từng ticker
    for ticker in tickers:
        table_name = get_table_name(ticker)
        
        # Kiểm tra ngày cuối cùng được cập nhật
        date_start = get_last_updated_date(table_name)
        
        # Nếu ticker chưa có trong bảng tickers, tìm ngày đầu tiên có dữ liệu
        if not date_start:
            date_start = find_first_data_date(ticker)
        else:
            date_start = date_start + timedelta(days=-1)
        
        # Tạo process cho mỗi lần tải
        process = multiprocessing.Process(
            target=download_ticker,
            args=(ticker, date_start)
        )
    
        try:
            process.start()
            # Chờ process hoàn thành với timeout 60s
            process.join(timeout=60)
            
            if process.is_alive():
                print(f"[Timeout] Bỏ qua {ticker} do vượt quá thời gian tải")
                process.terminate()
                process.join()
                
        except Exception as e:
            print(f"Lỗi khi xử lý {ticker}: {str(e)}")
            session.rollback()
            continue
        # Đọc dữ liệu từ file
        # Đường dẫn đến hai thư mục chứa dữ liệu
        daily_files = os.path.join(os.getcwd(), f"spot/daily/klines/{ticker}/1h")
        monthly_files = os.path.join(os.getcwd(), f"spot/monthly/klines/{ticker}/1h")

        # Lấy file CSV và xử lý lỗi
        daily_files = get_csv_files(daily_files)
        monthly_files = get_csv_files(monthly_files)
        all_files = daily_files + monthly_files

        if not all_files:
            print(f"Không có file CSV nào cho {ticker}")
            continue  # Bỏ qua ticker này

        # Đọc và kết hợp dữ liệu từ tất cả các file
        data = pd.concat([read_csv_file(file) for file in all_files], ignore_index=True)
        
        # Sắp xếp dữ liệu theo open_time
        data.sort_values(by='open_time', inplace=True)
        #print(data.head())

        # Tạo bảng nếu chưa tồn tại
        create_table_if_not_exists(table_name)
        
        try:
            save_data_to_table(table_name, data)
            update_last_updated_date(table_name, date.today())
            session.commit()  # Commit duy nhất
        except FileNotFoundError as e:
            print(f"Lỗi không tìm thấy file cho {ticker}: {str(e)}")
            continue
        except Exception as e:
            print(f"Lỗi nghiêm trọng khi xử lý {ticker}: {str(e)}")
            session.rollback()  # Rollback tất cả thay đổi nếu có lỗi
            continue
        finally:
            session.close()

# Chạy hàm chính
if __name__ == "__main__":
    # Khởi tạo engine và session
    engine = config.create_database_engine()
    Session = sessionmaker(bind=engine)
    session = Session()

    main()
    
    session.close()