#TA-lib
#url = 'https://anaconda.org/conda-forge/libta-lib/0.4.0/download/linux-64/libta-lib-0.4.0-h166bdaf_1.tar.bz2'
#!curl -L $url | tar xj -C /usr/lib/x86_64-linux-gnu/ lib --strip-components=1
#url = 'https://anaconda.org/conda-forge/ta-lib/0.4.19/download/linux-64/ta-lib-0.4.19-py310hde88566_4.tar.bz2'
#!curl -L $url | tar xj -C /usr/local/lib/python3.10/dist-packages/ lib/python3.10/site-packages/talib --strip-components=3

import talib as ta
import yfinance as yf
import polars as pl
import pandas as pd
import matplotlib.pyplot as plt
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import confusion_matrix, classification_report

nasdaq_data = yf.download('NDAQ', period = "max")

nasdaq_data = pl.from_pandas(nasdaq_data, include_index = True).rename({
    "('Adj Close', 'NDAQ')": "Adj Close",
    "('Close', 'NDAQ')": "Close",
    "('High', 'NDAQ')": "High",
    "('Low', 'NDAQ')": "Low",
    "('Open', 'NDAQ')": "Open",
    "('Volume', 'NDAQ')": "Volume",
})
#print(nasdaq_data)

nasdaq_data = (nasdaq_data
              .with_columns(
                  pl.Series(
                      name="RSI",
                      values = ta.RSI(nasdaq_data["Adj Close"].to_numpy(), timeperiod=14))))
print(nasdaq_data)
plt.figure(figsize=(12, 6))
plt.plot(nasdaq_data['Date'], nasdaq_data['RSI'], label='RSI', color='blue')
plt.axhline(y=70, color='red', linestyle='--', label='Overbought (70)')
plt.axhline(y=30, color='green', linestyle='--', label='Oversold (30)')
plt.legend()
plt.grid()
plt.show()

nasdaq_data = nasdaq_data.with_columns([
    pl.Series(
        name="MACD",
        values=ta.MACD(nasdaq_data["Adj Close"].to_numpy(),
                       fastperiod=12,
                       slowperiod=26,
                       signalperiod=9)[0]
    ),
    pl.Series(
        name="Signal_Line",
        values=ta.MACD(nasdaq_data["Adj Close"].to_numpy(),
                       fastperiod=12,
                       slowperiod=26,
                       signalperiod=9)[1]
    ),
    pl.Series(
        name="MACD_Hist",
        values=ta.MACD(nasdaq_data["Adj Close"].to_numpy(),
                       fastperiod=12,
                       slowperiod=26,
                       signalperiod=9)[2]
    )
])

nasdaq_data = nasdaq_data.with_columns([
    pl.Series(
        name="Volume MA",
        values=ta.SMA(nasdaq_data["Volume"].to_numpy().astype(float),
                      timeperiod=20))])

nasdaq_data = nasdaq_data.with_columns([
    pl.Series(
        name="Market_Trend",
        values=nasdaq_data["Adj Close"].shift(-1) > nasdaq_data["Adj Close"]).cast(pl.Int64)])

nasdaq_data = nasdaq_data.fill_nan(None).drop_nulls()

#print(nasdaq_data)

plt.figure(figsize=(12, 6))
plt.plot(nasdaq_data['Date'], nasdaq_data['MACD'], label='MACD', color='blue')
plt.plot(nasdaq_data['Date'], nasdaq_data['Signal_Line'], label='Signal Line', color='orange')
plt.bar(nasdaq_data['Date'], nasdaq_data['MACD_Hist'], label='MACD Histogram', color='gray', alpha=0.5)
plt.title('MACD over Time')
plt.legend()
plt.grid()
plt.show()

features = ["RSI", "MACD", "Signal_Line", "Volume", "Volume MA"]
X = nasdaq_data[features]
y = nasdaq_data["Market_Trend"]

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

model = LogisticRegression(max_iter=5000)
model.fit(X_train, y_train)

y_pred = model.predict(X_test)
#print(y_test.value_counts())
#print(y_pred)

conf_matrix = confusion_matrix(y_test, y_pred)
print("Confusion Matrix:\n", conf_matrix)

# Báo cáo hiệu suất
print("Classification Report:\n", classification_report(y_test, y_pred, zero_division=0))