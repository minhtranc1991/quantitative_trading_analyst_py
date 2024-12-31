import yfinance as yf
import polars as pl
import pandas as pd

stocks = ["SPY", "BAC", "AES", "DCOM"]

data = {stock: yf.download(stock, start="2003-01-01", end="2023-12-31") for stock in stocks}

dataframes = {
    stock: (
        pl.from_pandas(data[stock].reset_index())
        .rename({
            "Date": "Date",
            "Adj Close": "Adj Close",
            "Close": "Close",
            "High": "High",
            "Low": "Low",
            "Open": "Open",
            "Volume": "Volume",
        })
        .select([pl.col(col).alias(col.split()[0]) for col in data[stock].columns])
    )
    for stock in stocks
}

print(dataframes)