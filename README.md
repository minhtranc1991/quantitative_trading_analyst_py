# quantitative_trading_analyst_py

Bộ sưu tập script và notebook Python cho nghiên cứu giao dịch định lượng (chủ yếu thị trường crypto): thu thập dữ liệu, backtest chiến lược, tối ưu danh mục, machine learning.

## Cấu trúc

| Thư mục | Nội dung |
|---|---|
| `data/` | Tải dữ liệu lịch sử Binance (đa luồng), websocket, lưu vào MySQL |
| `portfolio/` | Tối ưu danh mục: **`Crypto_Portfolio_Optimization_V4_0.ipynb`** (walk-forward, bản mới nhất), V3 (bản gốc, in-sample), rolling optimization, HMM + Fama-French |
| `need-review/` | Chiến lược chờ review/backtest lại, gom theo nhóm (xem bên dưới) |
| `ml-trainning/` | Thử nghiệm ML: market vision, naive model, Calmar ratio |
| `LASSO-model/` | Mô hình LASSO với dữ liệu BingX/MySQL |
| `demo/` | Demo Sharpe ratio, multiprocessing, bot MT5 |
| `trading-game/` | Game mô phỏng giao dịch |
| `reverse-engineer-foundation/` | Phân tích dữ liệu giao dịch ví on-chain |

### `need-review/`

Mỗi chiến lược là một file; tên theo snake_case, hậu tố `_v2`, `_v3` là các biến thể khác nội dung cùng tên.

| Nhóm | Nội dung |
|---|---|
| `bb_rsi/` | Bollinger + RSI (`bb_rsi.py` kèm `indicators.py`, `binance_data_handle.py`) |
| `breakout/` | Breakout, retest, open range, ATR breakout |
| `ichimoku/` | Ichimoku, Kumo |
| `keltner_rsi/` | Keltner Channel + RSI và các biến thể SMMA/KC |
| `ma_crossover/` | Giao cắt MA, MA slope, TEMA |
| `mean_reversion/` | Mean reversion, mua đáy, pullback |
| `rsi/` | RSI thuần |
| `stat_arb/` | Stat arb, pair trading |
| `trend_following/` | Trend, momentum |
| `volume_candle/` | Theo khối lượng, độ dài nến |
| `vwap/` | VWAP |
| `misc/` | Còn lại |
| `_other/` | Không phải chiến lược giao dịch: danh mục, nghiên cứu, ML, tiện ích, cấu hình bot (`.json`), tài liệu |

Chưa chiến lược nào ở đây được kiểm chứng lại sau khi di chuyển.

## Crypto Portfolio Optimization V4

Walk-forward trên nến Binance: tại mỗi bước, fit weight trên `n_window` nến trước đó, áp cho nến kế tiếp.

- **Không look-ahead:** dữ liệu trễ `data_lag_days` (mặc định 1 ngày), không `bfill`, tải `display_candles + n_window + lag` nến.
- **Vũ trụ:** top coin CoinGecko, lọc trước (có cặp spot Binance, đủ lịch sử), cộng `extra_tickers` (EUR, vàng); mã bắt buộc có trọng số tối thiểu, các mã khác bị chặn trần (`other_max_weight`).
- **Strategy:** `opt_sharpe`, `opt_volatility`, `opt_optimal` (grid `max_assets`, chạy song song bằng `joblib`) và các benchmark `equal_weight`, `risk_parity`, `absolute_equal_dd_*` (MDD 5 năm tính từ nến ngày), `hold_<asset>`.
- **Rebalance:** theo chu kỳ `rebalance_days`, chỉ giao dịch khi weight lệch ≥ `rebalance_threshold`; có phí `fee` trên turnover.
- **Dùng:** chạy cell thư viện, chỉnh 2 cell PARAMS, chạy tuần tự các cell còn lại. Sau khi đổi dữ liệu hoặc nâng cấp thư viện, restart kernel.

## Cài đặt

```bash
pip install pandas numpy polars sqlalchemy python-dotenv binance-historical-data PyPortfolioOpt joblib plotly rich
```
Thêm gói theo từng script (scikit-learn, hmmlearn, MetaTrader5, ...).

## Cấu hình

Tạo `config.env` ở thư mục gốc (đã được gitignore) chứa thông tin kết nối MySQL / API key; `config.py` sẽ đọc file này.

## Chạy

Script ở `data/` và `portfolio/` dùng `import config`; chạy từ thư mục gốc, dạng module:

```bash
python -m data.save_data_binance_mysql
python -m portfolio.rolling_portfolio_optimization
```

Dữ liệu (`*.csv`, `*.zip`, thư mục `binance_data/`) không được commit.
