import polars as pl
from binance_historical_data import BinanceDataDumper

data_dumper = BinanceDataDumper(
    path_dir_where_to_dump=".",
    asset_class="um",  # spot, um, cm
    data_type="klines",  # aggTrades, klines, trades
    data_frequency="1d",
)

data_dumper = data_dumper.get_list_all_trading_pairs()
print(data_dumper)