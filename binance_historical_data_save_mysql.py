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
from sqlalchemy import Table, Column, MetaData, DateTime, Float, Integer, String, inspect, insert, Date
from sqlalchemy.dialects.mysql import insert as mysql_insert

# ---------------------------------------------------------------------------
# CẤU HÌNH VÀ KHỞI TẠO DATABASE
# ---------------------------------------------------------------------------
metadata = MetaData()

# Định nghĩa bảng tickers
tickers_table = Table(
    'tickers', metadata,
    Column('ticker', String(50), primary_key=True),
    Column('first_open_time', Date, nullable=False),
    Column('last_updated_date', Date, nullable=False),
    Column('name', String(50), nullable=False)
)

def create_engine_and_session():
    engine = config.create_database_engine()
    Session = sessionmaker(bind=engine)
    session = Session()
    return engine, session

def ensure_tickers_table_exists(engine):
    inspector = inspect(engine)
    if not inspector.has_table('tickers'):
        metadata.create_all(engine, [tickers_table])
        print("Bảng 'tickers' đã được tạo.")

# ---------------------------------------------------------------------------
# HÀM XỬ LÝ CSV VÀ THỜI GIAN
# ---------------------------------------------------------------------------
def detect_timestamp_unit(timestamp):
    num_digits = len(str(timestamp))
    if num_digits == 13:
        return 'ms'
    elif num_digits == 16:
        return 'us'
    else:
        raise ValueError(f"Timestamp không hợp lệ: {timestamp}")

def convert_timestamp(timestamp):
    unit = detect_timestamp_unit(timestamp)
    return pd.to_datetime(timestamp, unit=unit, errors='coerce')

def read_csv_file(file_path):
    df = pd.read_csv(file_path)
    df.columns = [
        "open_time", "open", "high", "low", "close", "volume",
        "close_time", "quote_asset_volume", "number_of_trades",
        "taker_buy_base_asset_volume", "taker_buy_quote_asset_volume", "ignore"
    ]
    df['open_time'] = df['open_time'].apply(convert_timestamp)
    df['close_time'] = df['close_time'].apply(convert_timestamp)
    return df

def get_csv_files(directory):
    try:
        if not os.path.exists(directory):
            print(f"Warning: Thư mục không tồn tại: {directory}")
            return []
        return [os.path.join(directory, f) for f in os.listdir(directory) if f.endswith('.csv')]
    except Exception as e:
        print(f"Lỗi khi đọc thư mục {directory}: {str(e)}")
        return []

def format_time(seconds):
    hours = int(seconds // 3600)
    minutes = int((seconds % 3600) // 60)
    secs = int(seconds % 60)
    return f"{hours:02d}:{minutes:02d}:{secs:02d}"

# ---------------------------------------------------------------------------
# HÀM XỬ LÝ DỮ LIỆU TICKER VÀ BẢNG CSDL
# ---------------------------------------------------------------------------
def get_list_all_trading_pairs():
    data_dumper = BinanceDataDumper(
        path_dir_where_to_dump=".",
        asset_class="spot",
        data_type="klines",
        data_frequency="1h",
    )
    return data_dumper.get_list_all_trading_pairs()

def filter_usdt_tickers(tickers):
    exclude_keywords = ["UPUSDT", "DOWNUSDT", "BEARUSDT", "BULLUSDT"]
    return [ticker for ticker in tickers if ticker.endswith("USDT") and not any(ex in ticker for ex in exclude_keywords)]

def get_table_name(ticker):
    return ticker.lower().replace("usdt", "_usdt")

def get_tickers_data(session, engine):
    try:
        ensure_tickers_table_exists(engine)
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

def find_first_data_date(ticker):
    data_dumper = BinanceDataDumper(
        path_dir_where_to_dump=".",
        asset_class="spot",
        data_type="klines",
        data_frequency="1h",
    )
    return data_dumper.get_min_start_date_for_ticker(ticker)

def create_table_if_not_exists(engine, table_name):
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

def save_data_to_table(session, engine, table_name, data):
    data_dict = data.to_dict(orient='records')
    if not data_dict:
        return
    table = Table(table_name, metadata, autoload_with=engine)
    stmt = insert(table).prefix_with("IGNORE").values(data_dict)
    session.execute(stmt)

def update_tickers_table(session, ticker, first_open_time, last_updated_date, name):
    stmt = mysql_insert(tickers_table).values(
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

# ---------------------------------------------------------------------------
# HÀM DOWNLOAD VÀ XỬ LÝ DỮ LIỆU TICKER
# ---------------------------------------------------------------------------
def download_ticker(ticker, date_start):
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

def run_download_ticker(ticker, date_start):
    try:
        download_ticker(ticker, date_start)
    except Exception as e:
        print(f"❌ Lỗi khi download {ticker}: {e}")

def process_csv_files(ticker):
    daily_path = os.path.join(os.getcwd(), f"spot/daily/klines/{ticker}/1h")
    monthly_path = os.path.join(os.getcwd(), f"spot/monthly/klines/{ticker}/1h")
    daily_files = get_csv_files(daily_path)
    monthly_files = get_csv_files(monthly_path)
    all_files = daily_files + monthly_files
    if not all_files:
        print(f"❗ Không có file CSV nào cho {ticker}")
        return None
    data = pd.concat([read_csv_file(file) for file in all_files], ignore_index=True)
    data.sort_values(by='open_time', inplace=True)
    return data

# ---------------------------------------------------------------------------
# HÀM CHÍNH
# ---------------------------------------------------------------------------
def main():
    start_time = time.time()
    print("Bắt đầu xử lý tickers...")

    # Lấy danh sách các ticker và lọc theo USDT
    all_tickers = get_list_all_trading_pairs()
    tickers = filter_usdt_tickers(all_tickers)
    print(f"Lấy danh sách tickers: {len(tickers)} tickers, thời gian: {format_time(time.time() - start_time)}")

    # Lấy dữ liệu tickers từ DB
    tickers_data = get_tickers_data(session, engine)
    print(f"Lấy dữ liệu tickers từ DB: thời gian: {format_time(time.time() - start_time)}")

    tickers_update_info = []
    total_tickers = len(tickers)

    # Cập nhật bảng tickers nếu dữ liệu chưa có
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
            update_tickers_table(session, ticker, first_open_time, last_updated_date, table_name)
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

    # Sắp xếp theo last_updated_date
    tickers_update_info.sort(key=lambda x: x['last_updated_date'])
    tickers = [info['ticker'] for info in tickers_update_info]
    print(f"Sắp xếp tickers theo ngày cập nhật: thời gian: {format_time(time.time() - start_time)}")
    print(f"Hoàn thành cập nhật tickers, tổng thời gian: {format_time(time.time() - start_time)}")

    # Xử lý từng ticker
    for i, ticker in enumerate(tickers):
        ticker_start_time = time.time()
        print(f"🔄 Đang xử lý {ticker}... (Ticker {i + 1}/{len(tickers)})")

        ticker_info = next((info for info in tickers_update_info if info['ticker'] == ticker), {})
        date_start = ticker_info.get('last_updated_date')
        first_open_time = ticker_info.get('first_open_time')

        if not date_start:
            date_start = find_first_data_date(ticker)
        else:
            date_start = date_start - timedelta(days=1)

        time_stop = 90  # thời gian timeout (giây)
        proc = multiprocessing.Process(target=run_download_ticker, args=(ticker, date_start))
        proc.start()
        proc.join(timeout=time_stop)

        if proc.is_alive():
            print(f"⚠️ Bỏ qua {ticker}: Thời gian chạy vượt quá {time_stop} giây")
            proc.terminate()  # Kết thúc process nếu vượt thời gian
            proc.join()       # Chờ cho process kết thúc
            continue

        # Xử lý dữ liệu CSV
        data = process_csv_files(ticker)
        if data is None:
            continue

        table_name = ticker_info.get('name')
        create_table_if_not_exists(engine, table_name)

        try:
            save_data_to_table(session, engine, table_name, data)
            last_updated_date = data['open_time'].max()
            update_tickers_table(session, ticker, first_open_time, last_updated_date, table_name)
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

        # Hiển thị thời gian xử lý và ước tính còn lại
        elapsed_time = time.time() - ticker_start_time
        total_elapsed = time.time() - start_time
        avg_ticker_time = total_elapsed / (i + 1)
        remaining_tickers = len(tickers) - i - 1
        estimated_time = avg_ticker_time * remaining_tickers

        print(f"✅ {ticker}: {format_time(elapsed_time)} | Đã xử lý: {format_time(total_elapsed)} | Còn lại: {format_time(estimated_time)} ({i + 1}/{len(tickers)})")

# ---------------------------------------------------------------------------
# MAIN - KHỞI CHẠY CHƯƠNG TRÌNH
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    engine, session = create_engine_and_session()
    main()
    session.close()
