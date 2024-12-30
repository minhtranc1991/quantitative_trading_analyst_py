import os
import polars as pl
import binance_historical_data as bhd
from binance_historical_data import BinanceDataDumper

if __name__ == "__main__":
    # Khởi tạo BinanceDataDumper
    data_dumper = BinanceDataDumper(
        path_dir_where_to_dump=".",
        asset_class="spot",  # spot, um, cm
        data_type="klines",  # aggTrades, klines, trades
        data_frequency="1m",
    )

    # Dump dữ liệu
    data_dumper.dump_data(
        tickers="BTCUSDT",
        date_start=None,
        date_end=None,
        is_to_update_existing=False,
        tickers_to_exclude=["UST"],
    )

    # Đọc dữ liệu từ các file CSV
    directory = "./spot/monthly/klines/BTCUSDT/1m"
    all_files = [os.path.join(directory, f) for f in os.listdir(directory) if f.endswith('.csv')]
    all_files.sort()

    btc = (
        pl.concat([pl.read_csv(file, has_header=False, columns=range(0, 11)) for file in all_files])
        .rename(
            {
                "column_1": "Open time",
                "column_2": "Open",
                "column_3": "High",
                "column_4": "Low",
                "column_5": "Close",
                "column_6": "Volume",
                "column_7": "Close time",
                "column_8": "Quote asset volume",
                "column_9": "Number of trades",
                "column_10": "Taker buy base asset volume",
                "column_11": "Taker buy quote asset volume",
            }
        )
        .with_columns(
            pl.from_epoch("Open time", "ms").alias("Open time"),
            pl.from_epoch("Close time", "ms").alias("Close time"),
        )
    )
    #print(btc.head())
    #print(btc.tail())

    btc_melted = btc.unpivot(
        index=['Open time'],
        on=['Close'],
        variable_name='Price_Type',
        value_name='Price_Value').rename(
            {
                "Price_Value": "Close"
            })
    
    btc_1h = btc_melted.group_by_dynamic(
        "Open time",
        every="1h",
        closed="right").agg([
            pl.col("Close").first().alias("Open"),
            pl.col("Close").max().alias("High"),
            pl.col("Close").min().alias("Low"),
            pl.col("Close").last().alias("Close"),
            # pl.col("RSI").last().alias("RSI"),
            # pl.col("pct_return").sum().alias("pct_return"),
            ])
    btc_strat = btc_1h.with_columns(pl.col("Close").ta.rsi(14).alias("RSI").fill_nan(None),((pl.col("Close")/pl.col("Close").shift())-1).alias("pct_return")).drop_nulls()
    btc_strat.head()
    btc_strat.tail()