# quantitative_trading_analyst_py

Bộ sưu tập script và notebook Python cho nghiên cứu giao dịch định lượng (chủ yếu thị trường crypto): thu thập dữ liệu, backtest chiến lược, tối ưu danh mục, machine learning.

## Cấu trúc

| Thư mục | Nội dung |
|---|---|
| `data/` | Tải dữ liệu lịch sử Binance (đa luồng), websocket, lưu vào MySQL |
| `need-review/` | Chiến lược chờ review/backtest lại, mỗi chiến lược một thư mục: `bb_rsi` (Bollinger + RSI, kèm helper), `vwap_ma_crossover` (giao cắt MVWAP), `breakout_retest` (breakout + retest 4H) |
| `portfolio/` | Tối ưu danh mục (rolling), HMM + Fama-French multifactor, notebook Crypto Portfolio Optimization |
| `ml-trainning/` | Thử nghiệm ML: market vision, naive model, Calmar ratio |
| `LASSO-model/` | Mô hình LASSO với dữ liệu BingX/MySQL |
| `demo/` | Demo Sharpe ratio, multiprocessing, bot MT5 |
| `trading-game/` | Game mô phỏng giao dịch |
| `reverse-engineer-foundation/` | Phân tích dữ liệu giao dịch ví on-chain |

## Cài đặt

```bash
pip install pandas numpy polars sqlalchemy python-dotenv binance-historical-data
```
Thêm gói theo từng script (scikit-learn, hmmlearn, MetaTrader5, ...).

## Cấu hình

Tạo `config.env` ở thư mục gốc (đã được gitignore) chứa thông tin kết nối MySQL / API key; `config.py` sẽ đọc file này.

## Chạy

Chạy từ thư mục gốc, dạng module, để `import config` hoạt động:

```bash
python -m data.save_data_binance_mysql
python -m portfolio.rolling_portfolio_optimization
```

Dữ liệu (`*.csv`, `*.zip`, `/data/*` không phải `.py`) không được commit.
