import os
import numpy as np
import pandas as pd 
import pickle
import matplotlib.pyplot as plt

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
    
# Load data from file
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

# Select cryptocurrency data
crypto_data = process_csv_files("BTCUSDT")

# Prepare data
data = crypto_data

class Leo_indicator:
    def __init__(self, data, period) -> None:
        self.high = data.high
        self.low = data.low
        self.close = data.close
        self.period = period

    def average_true_range(self):
        prev_close = self.close.shift(1)  
        high_low = self.high - self.low
        high_prev_close = np.abs(self.high - prev_close)
        low_prev_close = np.abs(self.low - prev_close)
        true_range = pd.Series(np.maximum(high_low, np.maximum(high_prev_close, low_prev_close)), index=self.close.index)
        average_true_range = true_range.ewm(span=self.period, adjust=False).mean()
        return average_true_range.ewm(span=self.period, adjust=False).mean()

    def ewm_stdev(self, type):
        if type == "close":
            ewm_std = self.close.ewm(span=self.period).std()
        elif type == "rets":
            rets = self.close.pct_change()
            ewm_std = rets.ewm(span=self.period).std()            
        else:
            raise ValueError("Invalid type specified; use 'close' or 'rets'.")
        return ewm_std.dropna()
        
    def calculate_bollinger_bands(self):
        std = self.close.rolling(self.period).std()
        bbm = self.close.ewm(span=self.period, adjust=False).mean()
        bbu = bbm + 2 * std
        bbl = bbm - 2 * std 
        return bbu.dropna(), bbm.dropna(), bbl.dropna()
    
    def neutral_phase(self):
        rsi = self.close.pct_change().rolling(self.period).mean()
        neutral_phase = pd.Series(np.where((rsi >= 30) & (rsi <= 70), 1, 0), index = rsi.index)
        return neutral_phase.dropna()
    
    def trend_detect(self):
        atr = self.average_true_range()
        ewm_std = self.ewm_stdev("close")
        trend_values = (atr / ewm_std).rolling(3).mean()
        trending = pd.Series(np.where(trend_values <= 0.4, 1, 0), index = trend_values.index)
        return trend_values, trending

class Main_strategy:
    def __init__(self, period, data) -> None:
        self.period = period
        self.data = data
        self.pos = 0  # 0 for no position, 1 for long, -1 for short
        self.entry_price = None
        self.entry_prices = []  # Store entry prices
        self.pnl = []  # Store unrealized PnL
        self.dates = []  # Store dates for plotting

    def update_data(self, idx):
        data = self.data.iloc[:idx]
        self.indicator = Leo_indicator(data, self.period)

    def indicator_use(self):
        self.neutral_phase = self.indicator.neutral_phase()
        _, self.trending = self.indicator.trend_detect()
        self.thresh_std = self.indicator.ewm_stdev("rets")
        self.bbu, _, self.bbl = self.indicator.calculate_bollinger_bands()

    def execute_trade(self, current_price):
        unrealized_pnl = 0
        if self.pos == 0 and self.trending.iloc[-1] == 1 and self.neutral_phase.iloc[-1] == 1: 
            if self.data['Low'].iloc[-1] * (1 - self.thresh_std.iloc[-1]) < self.bbl.iloc[-1]:
                self.pos = 1  
                self.entry_price = current_price 
                self.entry_prices.append(self.entry_price) 
            elif self.data['High'].iloc[-1] * (1 + self.thresh_std.iloc[-1]) > self.bbu.iloc[-1]:
                self.pos = -1
                self.entry_price = current_price
                self.entry_prices.append(self.entry_price)
        else:
            if self.entry_price is not None:  
                unrealized_pnl = (current_price - self.entry_price)/self.entry_price  if self.pos == 1 else (self.entry_price - current_price)/self.entry_price 

                # Define the effective TP and SL levels based on the entry price
                effective_tp = self.entry_price + self.thresh_std.iloc[-1] if self.pos == 1 else self.entry_price - self.thresh_std.iloc[-1]
                effective_sl = self.entry_price - self.thresh_std.iloc[-1] if self.pos == 1 else self.entry_price + self.thresh_std.iloc[-1]

                # Check for exit conditions
                if self.pos == 1:  # Long position
                    if current_price >= effective_tp:
                        self.pos = 0
                        self.entry_price = None
                    elif current_price <= effective_sl:
                        self.pos = 0
                        self.entry_price = None

                elif self.pos == -1:  # Short position
                    if current_price <= effective_tp:
                        self.pos = 0
                        self.entry_price = None
                    elif current_price >= effective_sl:
                        self.pos = 0
                        self.entry_price = None

        # Store unrealized PnL and date for plotting
        self.pnl.append(unrealized_pnl)
        
    def run(self):
        if len(self.data) < self.period:
            raise ValueError("Data length must be greater than or equal to the period.")

        # Start simulating
        for idx in range(self.period * 5, len(self.data)):
            self.update_data(idx)
            current_price = self.data['open'].iloc[idx]
            self.indicator_use()
            self.execute_trade(current_price)
            
# Run the strategy
strategy = Main_strategy(period=14, data=data)
strategy.run()

fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 10), sharex=False)

# Plot unrealized PnL
ax1.plot(np.cumsum(strategy.pnl), label='Unrealized PnL', color='blue')
ax1.axhline(0, color='red', linestyle='--', linewidth=1)
ax1.set_title('Unrealized PnL Over Time')
ax1.set_ylabel('Unrealized PnL')
ax1.legend()
ax1.grid()

# Price data
open_prices = data['Open']
close_prices = data['Close']
high_prices = data['High']
low_prices = data['Low']

li = Leo_indicator(data, 14)
# Calculate trend
_, a = li.trend_detect()
bbu, _, bbl = li.calculate_bollinger_bands()

# Create color array based on trend detection
colors = np.where(a == 1, 'red', 'blue')
colors = np.insert(colors, 0, "blue")

# Plot close prices and scatter points with color based on trend
ax2.plot(close_prices.index, close_prices, label='Close Price', color='gray', alpha=0.5)
ax2.plot(bbu, label='Close Price', color='red', alpha=0.5)
ax2.plot(bbl, label='Close Price', color='green', alpha=0.5)
ax2.scatter(close_prices.index, close_prices, color=colors, s=3)
ax2.set_title('Close Price with Trend Indicators')
ax2.set_ylabel('Price')
ax2.set_xlabel('Date')
ax2.legend()
ax2.grid()

# Adjust layout and show the plot
plt.tight_layout()
plt.show()