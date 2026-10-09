import pandas as pd
import numpy as np
import yfinance as yf
import plotly.graph_objects as go
import matplotlib.pyplot as plt
from hurst import compute_Hc
import warnings
import pickle
import os
from indicators import Indicators
from binance_data_handle import BinanceDataHandler
from multiprocessing import freeze_support
from datetime import datetime, timedelta
warnings.filterwarnings('ignore')

class MainStrategy:
    """
    Lớp MainStrategy dùng các chỉ báo từ Indicators để chạy chiến lược, tính toán vị thế, và vẽ biểu đồ equity.
    """
    def __init__(self, indicators: Indicators):
        self.ind = indicators
        self.data = self.ind.data
        self.close = self.ind.close
        self.thresh_std = self.ind.thresh_std
        self.neutral = self.ind.neutral
        self.trend = self.ind.trend
        self.bbl = self.ind.bbl
        self.bbu = self.ind.bbu
        self.returns = self.ind.returns

        # Xác định các mức Take Profit và Stop Loss
        self.tp = self.thresh_std * 2
        self.sl = -self.thresh_std

        # Khởi tạo các chuỗi vị thế và giá entry
        self.pos = pd.Series(0, index=self.data.index)
        self.entry_price = pd.Series(np.nan, index=self.data.index)

    def run_strategy(self):
        """
        Chạy chiến lược giao dịch với logic mean reversion dựa trên các điều kiện của RSI, trend và Bollinger Bands.
        """
        for i in range(1, len(self.close)):
            current_index = self.close.index[i]
            prev_index = self.close.index[i - 1]
            # print(f"Current Candle: {current_index}")
            # Nếu đang trong giai đoạn mean reversion: trend và neutral đều đúng
            if self.trend.loc[current_index] and self.neutral.loc[current_index]:
                # Kiểm tra điều kiện vào lệnh long:
                if self.data.loc[prev_index, 'Low'] * (1 - self.thresh_std.loc[prev_index]) < self.bbl.loc[current_index] and np.isnan(self.entry_price.loc[prev_index]):
                  self.pos.iloc[i] = 1  # Mở vị thế long
                  self.entry_price.iloc[i] = self.close.iloc[i]
                  # print(f"Open Long: {self.entry_price.iloc[i]}, {self.data.loc[current_index, 'Low'] * (1 - self.thresh_std.loc[current_index])}, {self.bbl.loc[current_index]}")
                # Kiểm tra điều kiện vào lệnh short:
                elif self.data.loc[prev_index, 'High'] * (1 + self.thresh_std.loc[prev_index]) > self.bbu.loc[current_index] and np.isnan(self.entry_price.loc[prev_index]):
                  self.pos.iloc[i] = -1  # Mở vị thế short
                  self.entry_price.iloc[i] = self.close.iloc[i]
                  # print(f"Open Short: {self.entry_price.iloc[i]}, {self.data.loc[current_index, 'High'] * (1 + self.thresh_std.loc[current_index])}, {self.bbu.loc[current_index]}")
                else:
                  self.pos.iloc[i] = self.pos.iloc[i - 1]
                  self.entry_price.iloc[i] = self.entry_price.iloc[i - 1]

            else:
                # Nếu không thỏa điều kiện vào lệnh, giữ nguyên vị thế và giá entry từ phiên trước
                self.pos.iloc[i] = self.pos.iloc[i - 1]
                self.entry_price.iloc[i] = self.entry_price.iloc[i - 1]

            # Check for stop-loss and take-profit
            # --- Kiểm tra thoát lệnh cho vị thế Long ---
            if self.pos.iloc[i] == 1:
                # Nếu giá hiện tại đạt mức Take Profit (TP) cho lệnh Long
                if self.close.iloc[i] >= self.entry_price.iloc[i] * (1 + self.tp.iloc[i]):
                    # print(f"Current Candle: {current_index} - TP Long: Entry - {self.entry_price.iloc[i]}, TP - {self.close.iloc[i]}")
                    self.pos.iloc[i] = 0                  # Đóng vị thế
                    self.entry_price.iloc[i] = np.nan     # Reset giá vào lệnh
                # Nếu giá hiện tại chạm Stop Loss (SL) cho lệnh Long
                elif self.close.iloc[i] <= self.entry_price.iloc[i] * (1 + self.sl.iloc[i]):
                    # print(f"Current Candle: {current_index} - SL Long: Entry - {self.entry_price.iloc[i]}, SL - {self.close.iloc[i]}")
                    self.pos.iloc[i] = 0                  # Đóng vị thế
                    self.entry_price.iloc[i] = np.nan     # Reset giá vào lệnh

            # --- Kiểm tra thoát lệnh cho vị thế Short ---
            elif self.pos.iloc[i] == -1:
                # Nếu giá hiện tại đạt mức Take Profit (TP) cho lệnh Short
                if self.close.iloc[i] <= self.entry_price.iloc[i] * (1 - self.tp.iloc[i]):
                    # print(f"Current Candle: {current_index} - TP Short: Entry - {self.entry_price.iloc[i]}, TP - {self.close.iloc[i]}")
                    self.pos.iloc[i] = 0                  # Đóng vị thế
                    self.entry_price.iloc[i] = np.nan     # Reset giá vào lệnh
                # Nếu giá hiện tại chạm Stop Loss (SL) cho lệnh Short
                elif self.close.iloc[i] >= self.entry_price.iloc[i] * (1 - self.sl.iloc[i]):
                    # print(f"Current Candle: {current_index} - SL Short: Entry - {self.entry_price.iloc[i]}, SL - {self.close.iloc[i]}")
                    self.pos.iloc[i] = 0                  # Đóng vị thế
                    self.entry_price.iloc[i] = np.nan     # Reset giá vào lệnh

        # Reset các vị thế ban đầu nếu cần (ở đây reset 100 phiên đầu)
        self.pos.iloc[0:100] = 0

        # Tính toán Equity curve dựa trên lợi nhuận chưa hiện thực
        self.unrlz_pnls = self.pos * self.returns * 0.96
        self.unrlz_pnls_cum = self.unrlz_pnls.cumsum() * 100

        # print("Số lượng lệnh được mở:", self.entry_price.notna().sum())
        # print("Số lượng phiên có vị thế long:", (self.pos == 1).sum())
        # print("Số lượng phiên có vị thế short:", (self.pos == -1).sum())
        # print("Số lượng phiên không có lệnh:", (self.pos == 0).sum())

    def plot_results(self):
      df = self.data.copy()
      
      df['Entry'] = self.entry_price
      df['TP'] = self.entry_price * (1 + self.tp)
      df['SL'] = self.entry_price * (1 + self.sl)
      
      df['BBU'] = self.bbu
      df['BBL'] = self.bbl
      df['LowOvershoot'] = df['Low'] * (1 - self.thresh_std)
      df['HighOvershoot'] = df['High'] * (1 + self.thresh_std)
      # for i in range(len(df)):
      #   print(df.loc[i, ['Entry','TP','SL']])
      fig = go.Figure()

      # Biểu đồ nến
      fig.add_trace(go.Candlestick(
          x=df['open_time'],
          open=df['Open'],
          high=df['High'],
          low=df['Low'],
          close=df['Close'],
          name='Candles'
      ))

      # Các đường bổ sung
      fig.add_trace(go.Scatter(x=df['open_time'], y=df['Entry'], name='Entry', line=dict(color='green')))
      fig.add_trace(go.Scatter(x=df['open_time'], y=df['TP'], name='Take Profit', line=dict(color='green', dash='dash')))
      fig.add_trace(go.Scatter(x=df['open_time'], y=df['SL'], name='Stop Loss', line=dict(color='red', dash='dash')))
      fig.add_trace(go.Scatter(x=df['open_time'], y=df['BBU'], name='Bollinger Upper', line=dict(color='white')))
      fig.add_trace(go.Scatter(x=df['open_time'], y=df['BBL'], name='Bollinger Lower', line=dict(color='white')))
      fig.add_trace(go.Scatter(x=df['open_time'], y=df['LowOvershoot'], name='Low Overshoot', line=dict(color='purple', dash='dot')))
      fig.add_trace(go.Scatter(x=df['open_time'], y=df['HighOvershoot'], name='High Overshoot', line=dict(color='orange', dash='dot')))


      fig.update_layout(
          title='Interactive Candle Chart with Strategy Overlay',
          xaxis_title='Time',
          yaxis_title='Price',
          xaxis_rangeslider_visible=False,
          template='plotly_dark',
          height=700
      )

      fig.show()

      # Create a figure and axes
      fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 8), sharex=True)

      # Plot Position
      ax1.plot(strategy.data['open_time'], strategy.pos, label="Position", color='blue')
      ax1.set_ylabel("Position")
      ax1.legend()

      # Plot Equity Curve
      ax2.plot(strategy.data['open_time'], strategy.unrlz_pnls_cum, label="Equity Curve", color='green')
      ax2.set_xlabel("Time")
      ax2.set_ylabel("Equity (%)")
      ax2.legend()

      # Format x-axis with dates
      fig.autofmt_xdate()
      plt.tight_layout()
      plt.show()

    def print_summary(self):
      """
      In ra các chỉ số tóm tắt của chiến lược, bao gồm:
      - Sharpe Ratio và tổng lợi nhuận của cả ticker và chiến lược
      - Max Drawdown
      - Longest Drawdown Duration
      """
      # Sharpe và tổng returns
      sharpe_returns = self.returns.mean() / self.returns.std() * np.sqrt(len(self.returns))
      sum_returns = self.returns.sum()
      sharpe_strategy = self.unrlz_pnls.mean() / self.unrlz_pnls.std() * np.sqrt(len(self.unrlz_pnls))
      sum_pnls = self.unrlz_pnls.sum() * 100

      # Equity curve
      equity_curve = self.unrlz_pnls_cum

      # Tính drawdown
      peak = equity_curve.cummax()
      drawdown = equity_curve - peak
      max_drawdown = drawdown.min()

      # Tính thời gian dài nhất equity chưa phục hồi (longest drawdown duration)
      drawdown_duration = 0
      max_duration = 0
      for i in range(1, len(equity_curve)):
          if equity_curve[i] < peak[i]:
              drawdown_duration += 1
              max_duration = max(max_duration, drawdown_duration)
          else:
              drawdown_duration = 0

      print(f'Sharpe Ratio Ticker: {sharpe_returns:.4f}')
      print(f'Sum Ticker Returns: {sum_returns:.4f}')
      print(f'Sharpe Ratio Strategy: {sharpe_strategy:.4f}')
      print(f'Sum Strategy PnLs: {sum_pnls:.4f}%')
      print(f'Max Drawdown: {max_drawdown:.4f}%')
      print(f'Longest Drawdown Duration: {max_duration} bars')

if __name__ == '__main__':
    freeze_support()

    ticker = "BTCUSDT"
    data_frequency = "15m"
    
    handler = BinanceDataHandler(ticker, data_frequency)
    
    data = handler.load_data()
    data = data[:400].reset_index(drop=True)
    
    # # Tạo đối tượng chỉ báo và tính toán các chỉ báo cần thiết
    indicators = Indicators(data)
    indicators.compute_indicators()

    # # Tạo đối tượng chiến lược, chạy chiến lược, và hiển thị kết quả
    strategy = MainStrategy(indicators)
    strategy.run_strategy()
    strategy.plot_results()
    strategy.print_summary()
