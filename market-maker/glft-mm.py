import os
import numpy as np
from matplotlib import pyplot as plt
from datetime import date, timedelta
from numba import njit
from hftbacktest import BUY_EVENT
from hftbacktest import BacktestAsset, ROIVectorMarketDepthBacktest

@njit
def measure_trading_intensity_and_volatility(hbt):
    '''
    Đo intensity: mức độ "sâu" của các lệnh market order (mua hoặc bán) so với mid-price.
    Đo volatility: thay đổi của mid-price theo thời gian.
    Cách đo:
      Cứ mỗi 100ms (100_000_000 nanoseconds),
      Ghi lại:
        Độ sâu tối đa của các giao dịch mới so với mid-price (arrival depth).
        Độ thay đổi của mid-price (mid-price change).
    '''
    tick_size = hbt.depth(0).tick_size
    arrival_depth = np.full(10_000_000, np.nan, np.float64)
    mid_price_chg = np.full(10_000_000, np.nan, np.float64)

    t = 0
    prev_mid_price_tick = np.nan
    mid_price_tick = np.nan

    # Checks every 100 milliseconds.
    while hbt.elapse(100_000_000) == 0:
        #--------------------------------------------------------
        # Records market order's arrival depth from the mid-price.
        if not np.isnan(mid_price_tick):
            depth = -np.inf
            for last_trade in hbt.last_trades(0):
                trade_price_tick = last_trade.px / tick_size

                if last_trade.ev & BUY_EVENT == BUY_EVENT:
                    depth = np.nanmax([trade_price_tick - mid_price_tick, depth])
                else:
                    depth = np.nanmax([mid_price_tick - trade_price_tick, depth])
            arrival_depth[t] = depth

        hbt.clear_last_trades(0)

        depth = hbt.depth(0)

        best_bid_tick = depth.best_bid_tick
        best_ask_tick = depth.best_ask_tick

        prev_mid_price_tick = mid_price_tick
        mid_price_tick = (best_bid_tick + best_ask_tick) / 2.0

        # Records the mid-price change for volatility calculation.
        mid_price_chg[t] = mid_price_tick - prev_mid_price_tick

        t += 1
        if t >= len(arrival_depth) or t >= len(mid_price_chg):
            raise Exception
    return arrival_depth[:t], mid_price_chg[:t]

@njit
def measure_trading_intensity(order_arrival_depth, out):
    '''
    Input:
      order_arrival_depth: một mảng (np.ndarray) chứa độ sâu (depth) theo đơn vị tick (khoảng cách từ mid-price đến giá giao dịch) của từng lệnh thị trường (market order) tại các thời điểm.
      out: một mảng (np.ndarray) để ghi kết quả đếm số lượng lệnh đã chạm tới mỗi mức giá.
    Output:
    Trả về phần out đã cập nhật, chỉ đến mức tick xa nhất có lệnh (max_tick).
    '''
    max_tick = 0
    for depth in order_arrival_depth:
        if not np.isfinite(depth):
            continue

        # Sets the tick index to 0 for the nearest possible best price
        # as the order arrival depth in ticks is measured from the mid-price
        tick = round(depth / .5) - 1

        # In a fast-moving market, buy trades can occur below the mid-price (and vice versa for sell trades)
        # since the mid-price is measured in a previous time-step;
        # however, to simplify the problem, we will exclude those cases.
        if tick < 0 or tick >= len(out):
            continue

        # All of our possible quotes within the order arrival depth,
        # excluding those at the same price, are considered executed.
        out[:tick] += 1

        max_tick = max(max_tick, tick)
    return out[:max_tick]

backtest_date = '20250118'
eod = date.fromisoformat(backtest_date) - timedelta(days=1)
ticker = 'btcusdt'
file_name = ticker + '_' + backtest_date
eod_file_name = ticker + '_' + eod.strftime('%Y%m%d') + '_eod'

files = [
    f'data/{file_name}.npz',
    f'data/{eod_file_name}.npz',
    f'latency/feed_latency_{backtest_date}.npz'
]

for f in files:
    print(f"{f}: {'Found' if os.path.exists(f) else 'Not Found'}")

asset = (
    BacktestAsset()
        .data([
            f'data/{file_name}.npz'
        ])
        .initial_snapshot(f'data/{file_name}_eod.npz')
        .linear_asset(1.0)
        .intp_order_latency([
            f'latency/feed_latency_{backtest_date}.npz'
        ])
        .power_prob_queue_model(2.0)
        .no_partial_fill_exchange()
        .trading_value_fee_model(-0.00005, 0.0007)
        .tick_size(0.01)
        .lot_size(0.001)
        .roi_lb(0.0)
        .roi_ub(3000.0)
        .last_trades_capacity(10000)
)

hbt = ROIVectorMarketDepthBacktest([asset])

arrival_depth, mid_price_chg = measure_trading_intensity_and_volatility(hbt)

_ = hbt.close()

tmp = np.zeros(500, np.float64)

# Measures trading intensity (lambda) for the first 10-minute window.
lambda_ = measure_trading_intensity(arrival_depth[:6_000], tmp)

# Since it is measured for a 10-minute window, divide by 600 to convert it to per second.
lambda_ /= 600

# Creates ticks from the mid-price.
ticks = np.arange(len(lambda_)) + .5

print(f"Ticks: {ticks}")
print(f"Lambda: {lambda_}")

plt.figure(figsize=(16, 9))
plt.plot(ticks, lambda_)
plt.xlabel('$ \delta $ (ticks from the mid-price)')
plt.ylabel('Count (per second)')
plt.show()