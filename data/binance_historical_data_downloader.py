import os
import shutil
from binance_historical_data import BinanceDataDumper
import datetime
import multiprocessing

if __name__ == "__main__":
    multiprocessing.freeze_support()

    temp_output_dir = "binance_temp_data"
    final_output_dir = "binance_monthly_data"

    for path in [temp_output_dir, final_output_dir]:
        if not os.path.exists(path):
            os.makedirs(path)

    now = datetime.datetime.now()
    first_day_of_month = datetime.date(now.year, now.month, 1)
    last_day_of_month = (first_day_of_month + datetime.timedelta(days=32)).replace(day=1) - datetime.timedelta(days=1)

    data_dumper = BinanceDataDumper(
        path_dir_where_to_dump=temp_output_dir,
        asset_class='spot',
        data_type='klines',
        data_frequency='1h'
    )

    tickers = data_dumper.get_list_all_trading_pairs()
    excluded_tickers = ["UPUSDT", "DOWNUSDT", "BEARUSDT", "BULLUSDT"]
    tickers = [
        ticker
        for ticker in tickers
        if "USDT" in ticker and not any(excluded in ticker for excluded in excluded_tickers)
    ]

    # Tải toàn bộ dữ liệu
    data_dumper.dump_data(
        tickers=tickers,
        date_start=first_day_of_month,
        date_end=last_day_of_month,
        is_to_update_existing=True
    )

    # Di chuyển dữ liệu về thư mục đích
    spot_path = os.path.join(temp_output_dir, "spot", "daily", "klines")

    if os.path.exists(spot_path):
        for ticker in tickers:
            temp_ticker_dir = os.path.join(spot_path, ticker, "1h")
            final_ticker_dir = os.path.join(final_output_dir, "spot", ticker)

            if os.path.exists(temp_ticker_dir):
                if not os.path.exists(final_ticker_dir):
                    shutil.move(temp_ticker_dir, final_ticker_dir)
                else:
                    for file in os.listdir(temp_ticker_dir):
                        src_file = os.path.join(temp_ticker_dir, file)
                        dest_file = os.path.join(final_ticker_dir, file)

                        if os.path.exists(dest_file):
                            with open(src_file, 'r') as src, open(dest_file, 'a') as dest:
                                next(src)  # Bỏ qua header của file mới nếu đã có trong file cũ
                                dest.write(src.read())
                        else:
                            shutil.move(src_file, dest_file)

    # Xóa toàn bộ thư mục tạm sau khi hoàn tất
    shutil.rmtree(temp_output_dir)
