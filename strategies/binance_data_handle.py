import os
import pandas as pd
from datetime import datetime
from binance_historical_data import BinanceDataDumper

class BinanceDataHandler:
    def __init__(self, ticker, data_frequency="1h"):
        self.ticker = ticker
        self.data_frequency = data_frequency
        self.base_path = os.getcwd()

    def detect_timestamp_unit(self, timestamp):
        num_digits = len(str(timestamp))
        if num_digits == 13:
            return 'ms'
        elif num_digits == 16:
            return 'us'
        else:
            raise ValueError(f"Timestamp không hợp lệ: {timestamp}")

    def convert_timestamp(self, timestamp):
        unit = self.detect_timestamp_unit(timestamp)
        return pd.to_datetime(timestamp, unit=unit, errors='coerce')

    def download_data(self, date_start, date_end):
        data_dumper = BinanceDataDumper(
            path_dir_where_to_dump=".",
            asset_class="spot",
            data_type="klines",
            data_frequency=self.data_frequency,
        )
        date_start = datetime.strptime(date_start, "%Y-%m-%d").date()
        date_end = datetime.strptime(date_end, "%Y-%m-%d").date()
        data_dumper.dump_data(
            tickers=self.ticker,
            date_start=date_start,
            date_end=date_end,
            is_to_update_existing=False,
        )

    def read_csv_file(self, file_path):
        df = pd.read_csv(file_path)
        df.columns = [
            "open_time", "Open", "High", "Low", "Close", "volume",
            "close_time", "quote_asset_volume", "number_of_trades",
            "taker_buy_base_asset_volume", "taker_buy_quote_asset_volume", "ignore"
        ]
        df['open_time'] = df['open_time'].apply(self.convert_timestamp)
        df['close_time'] = df['close_time'].apply(self.convert_timestamp)
        return df

    def get_csv_files(self, directory):
        try:
            if not os.path.exists(directory):
                print(f"⚠️  Thư mục không tồn tại: {directory}")
                return []
            return [os.path.join(directory, f) for f in os.listdir(directory) if f.endswith('.csv')]
        except Exception as e:
            print(f"❌ Lỗi khi đọc thư mục {directory}: {str(e)}")
            return []

    def load_data(self):
        daily_path = os.path.join(self.base_path, f"spot/daily/klines/{self.ticker}/{self.data_frequency}")
        monthly_path = os.path.join(self.base_path, f"spot/monthly/klines/{self.ticker}/{self.data_frequency}")
        daily_files = self.get_csv_files(daily_path)
        monthly_files = self.get_csv_files(monthly_path)
        all_files = daily_files + monthly_files
        if not all_files:
            print(f"❗ Không có file CSV nào cho {self.ticker}")
            return None
        data = pd.concat([self.read_csv_file(file) for file in all_files], ignore_index=True)
        data.sort_values(by='open_time', inplace=True)
        return data

# --- Sử dụng class ---
# if __name__ == '__main__':
#     from multiprocessing import freeze_support
#     freeze_support()

#     handler = BinanceDataHandler(ticker='BTCUSDT', data_frequency='15m')
#     handler.download_data("2025-02-15", "2025-03-01")
#     crypto_data = handler.load_data()

#     # Chuẩn bị dữ liệu mẫu
#     if crypto_data is not None:
#         data = crypto_data[:400].reset_index(drop=True)
#         print(data)