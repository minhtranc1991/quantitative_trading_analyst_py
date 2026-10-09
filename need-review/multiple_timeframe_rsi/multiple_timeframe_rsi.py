import os
import itertools
import numpy as np
import pandas as pd
import polars as pl
import matplotlib.pyplot as plt
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
    btc_date = btc_strat[["Open time"]]
    btc_return = btc_strat[["pct_return"]]
    btc_rsi = btc_strat[["RSI"]]

    btc_strat_pd = btc_strat.to_pandas()
    btc_date_pd = btc_strat_pd[["Open time"]]
    btc_return_pd = btc_strat_pd[["pct_return"]]
    btc_rsi_pd = btc_strat_pd[["RSI"]]

    btc_strat[["Open time"]].to_numpy()

    btc_rsi_pd.iloc[1]

    date = []
pnl = []

rsi_long_threshold = 70
rsi_short_threshold = 15
hold_time = 40
#Counter
position = 0
hold_counter = 0
in_position = False

for i in range(len(btc_date)):

    date.append(btc_date[i].item())

    if in_position:
        hold_counter += 1
        pnl.append(btc_return[i].item()*position)
        if hold_counter >= hold_time:
            in_position = False
            hold_counter = 0
            continue
    else:
        pnl.append(0)

    if (btc_rsi[i].item() > rsi_long_threshold) and (btc_rsi[i-1].item() < rsi_long_threshold):
        in_position = True
        position = 1
        hold_counter = 0

    elif (btc_rsi[i].item() < rsi_short_threshold) and (btc_rsi[i-1].item() > rsi_short_threshold):
        in_position = True
        position = -1
        hold_counter = 0

    pnl_cal = np.cumprod(1+ np.array(pnl))

    plt.figure(figsize=(12, 6))
    plt.plot(date, pnl_cal-1)
    plt.xlabel("Date")
    plt.ylabel("Unrlz Pnl")
    plt.title("Unrlz Pnl Over Time")
    plt.grid(True)
    plt.show()

    # Convert pnl_cal to a Pandas Series
    pnl_cal_series = pl.Series(pnl_cal)

    # Calculate the drawdown using the Pandas Series
    drawdown = (pnl_cal_series - pnl_cal_series.cum_max()) / pnl_cal_series.cum_max()

    # Plot the drawdown
    plt.figure(figsize=(12, 6))
    plt.plot(date, drawdown)
    plt.xlabel("Date")
    plt.ylabel("Drawdown")
    plt.title("Drawdown Plot")
    plt.grid(True)
    plt.show()

    def calculate_max_drawdown(pnl_cal):
        peak = pnl_cal[0]
        max_drawdown = 0
        for i in range(1, len(pnl_cal)):
            if pnl_cal[i] > peak:
                peak = pnl_cal[i]
            # Check for zero and invalid values in peak and pnl_cal[i]
            if peak != 0 and not (np.isnan(peak) or np.isinf(peak) or np.isnan(pnl_cal[i]) or np.isinf(pnl_cal[i])):
                drawdown = (peak - pnl_cal[i]) / peak
                if drawdown > max_drawdown:
                    max_drawdown = drawdown
        return max_drawdown

    def calculate_longest_drawdown(pnl_cal):
        peak = pnl_cal[0]
        max_drawdown = 0
        longest_drawdown_duration = 0
        current_drawdown_duration = 0
        for i in range(1, len(pnl_cal)):
            if pnl_cal[i] > peak:
                peak = pnl_cal[i]
                current_drawdown_duration = 0
            else:
                # Check for zero and invalid values in peak and pnl_cal[i]
                if peak != 0 and not (np.isnan(peak) or np.isinf(peak) or np.isnan(pnl_cal[i]) or np.isinf(pnl_cal[i])):
                    drawdown = (peak - pnl_cal[i]) / peak
                    current_drawdown_duration += 1
                    if drawdown > max_drawdown:
                        max_drawdown = drawdown
                        longest_drawdown_duration = current_drawdown_duration
        return longest_drawdown_duration


    def calculate_sharpe_ratio(pnl_cal, risk_free_rate=0.02):
        returns = np.diff(pnl_cal)
        excess_returns = returns - risk_free_rate / 252  # Assuming daily returns
        sharpe_ratio = np.sqrt(252) * np.mean(excess_returns) / np.std(excess_returns)
        return sharpe_ratio

    # Assuming 'pnl_cal' is defined from the previous code
    max_drawdown = calculate_max_drawdown(pnl_cal)
    longest_drawdown = calculate_longest_drawdown(pnl_cal)
    sharpe = calculate_sharpe_ratio(pnl)

    print(f"Max Drawdown: {max_drawdown}")
    print(f"Longest Drawdown Duration: {longest_drawdown}")
    print(f"Sharpe Ratio: {sharpe}")

    def run_backtest(btc_date, btc_return, btc_rsi, rsi_long_threshold, rsi_short_threshold, hold_time):
        date = []
        pnl = []
        position = 0
        hold_counter = 0
        in_position = False

        for i in range(len(btc_date)):
            date.append(btc_date.iloc[i])

            if in_position:
                hold_counter += 1
                pnl.append(btc_return.iloc[i] * position)
                if hold_counter >= hold_time:
                    in_position = False
                    hold_counter = 0
                    continue
            else:
                pnl.append(0)

            if (btc_rsi.iloc[i] >= rsi_long_threshold) and (btc_rsi.iloc[i-1] < rsi_long_threshold):
                in_position = True
                position = 1
                hold_counter = 0
            elif (btc_rsi.iloc[i] <= rsi_short_threshold) and (btc_rsi.iloc[i-1] > rsi_short_threshold):
                in_position = True
                position = -1
                hold_counter = 0

        return date, pnl

    # Define parameter ranges
    rsi_long_thresholds = np.arange(60, 80, 5)  # Example range
    rsi_short_thresholds = np.arange(20, 40, 5) # Example range
    hold_times = np.arange(10, 30, 5)          # Example range

    # Create parameter combinations
    parameter_combinations = list(itertools.product(rsi_long_thresholds, rsi_short_thresholds, hold_times))

    # Store results
    results = []

    # Loop through combinations and run backtest
    for rsi_long, rsi_short, hold_time in parameter_combinations:
        dates, pnl = run_backtest(btc_date_pd, btc_return_pd, btc_rsi_pd, rsi_long, rsi_short, hold_time)

        # Create a DataFrame for the results of this run
        result_df = pd.DataFrame({'date': dates, 'pnl': pnl})

        # Calculate cumulative PnL
        result_df['cumulative_pnl'] = result_df['pnl'].cumsum()

        # Calculate performance metrics
        max_drawdown = calculate_max_drawdown(result_df['cumulative_pnl'])
        longest_drawdown = calculate_longest_drawdown(result_df['cumulative_pnl'])
        sharpe = calculate_sharpe_ratio(result_df['cumulative_pnl'])

        # Store results
        results.append({
            'rsi_long_threshold': rsi_long,
            'rsi_short_threshold': rsi_short,
            'hold_time': hold_time,
            'max_drawdown': max_drawdown,
            'longest_drawdown': longest_drawdown,
            'sharpe_ratio': sharpe
        })

    # Convert results to DataFrame
    results_df = pd.DataFrame(results)

    # Analyze results (example: sort by Sharpe Ratio)
    results_df_sorted = results_df.sort_values('sharpe_ratio', ascending=False) 