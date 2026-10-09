import numpy as np
import pandas as pd
import pandas_ta as ta

class Indicators:
    """
    Lớp Indicators tính toán các chỉ báo kỹ thuật từ dữ liệu giá.
    """
    def __init__(self, data):
        """
        Khởi tạo với DataFrame chứa các cột: 'Open', 'Close', 'High', 'Low'
        """
        self.data = data.copy()
        self.open = data['Open']
        self.close = data['Close']
        self.high = data['High']
        self.low = data['Low']
        self.returns = self.close.pct_change()

    @staticmethod
    def calculate_true_range(high, low, prev_close):
        """Tính True Range dựa vào high, low và giá đóng cửa của phiên trước."""
        high_low = high - low
        high_prev_close = np.abs(high - prev_close)
        low_prev_close = np.abs(low - prev_close)
        return np.maximum(high_low, np.maximum(high_prev_close, low_prev_close))

    def calculate_atr(self, period=14):
        """Tính Average True Range (ATR) dựa vào period."""
        prev_close = self.close.shift(1)  # Giá đóng cửa phiên trước
        true_range = self.calculate_true_range(self.high, self.low, prev_close)
        atr = ta.rma(true_range, period)
        return atr

    def compute_indicators(self):
        """Tính toán các chỉ báo cần thiết và lưu thành các thuộc tính nội bộ."""
        # Bollinger Bands: EMA 20 và độ lệch chuẩn
        self.bb = ta.ema(self.close, 20)
        self.bb_std = self.close.rolling(20).std()
        self.bbu = self.bb + 2 * self.bb_std  # Upper band
        self.bbl = self.bb - 2 * self.bb_std  # Lower band

        # RSI với chu kỳ 14
        self.rsi = ta.rsi(self.close, 14)

        # ATR tính từ True Range và sau đó áp dụng EMA
        self.atr = ta.ema(self.calculate_atr(period=14), 14)

        # Tính độ lệch chuẩn của giá sử dụng EWM
        self.std = self.close.ewm(span=14).std()

        # Tính giá trị trend: tỷ số ATR/Std rồi lấy trung bình trượt 3 kỳ
        self.trend_values = (self.atr / self.std).rolling(3).mean()

        # Điều kiện "neutral" dựa trên RSI
        self.neutral = (self.rsi >= 30) & (self.rsi <= 70)

        # Giới hạn của trend để xác định pha mean reversion
        self.trend_threshold = 0.6
        self.trend = self.trend_values <= self.trend_threshold

        # Tính rolling standard deviation từ returns, dùng để xác định mức SL và TP
        self.thresh_std = self.returns.ewm(span=14).std() * 4