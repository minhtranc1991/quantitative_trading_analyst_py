import config
import pandas as pd
from sqlalchemy import Table, MetaData, select
from sqlalchemy.orm import sessionmaker
from pypfopt import expected_returns, CovarianceShrinkage, EfficientFrontier

def fetch_ticker_data(start_date="2020-09-01"):
    engine = config.create_database_engine()
    Session = sessionmaker(bind=engine)
    session = Session()

    # Khởi tạo metadata và bảng
    db_metadata = MetaData()
    tickers = Table('tickers', db_metadata, autoload_with=engine, schema='quant_trading')

    # Tạo truy vấn
    query = select(tickers.c.name).where(
        (tickers.c.first_open_time <= start_date) &
        ~tickers.c.ticker.like("%bear_%") &
        ~tickers.c.ticker.like("%bull_%") &
        ~tickers.c.ticker.like("%up_%") &
        ~tickers.c.ticker.like("%down_%")
    )

    # Thực thi truy vấn và lấy kết quả
    result = session.execute(query)
    tickers_list = [row[0] for row in result]

    # Lấy dữ liệu open_time, close cho mỗi ticker
    data_frames = []
    for ticker in tickers_list:
        ticker_table = Table(ticker, db_metadata, autoload_with=engine, schema='quant_trading')
        candle_query = select(ticker_table.c.open_time, ticker_table.c.close).where(ticker_table.c.open_time >= start_date)
        candle_result = session.execute(candle_query)
        df = pd.DataFrame(candle_result.fetchall(), columns=['open_time', 'close']).set_index('open_time')
        df.rename(columns={'close': ticker}, inplace=True)
        data_frames.append(df)

    # Gộp dữ liệu thành DataFrame tổng hợp và fill NaN bằng 0
    df = pd.concat(data_frames, axis=1).reset_index()
    df.fillna(method='ffill', inplace=True)
    df.fillna(0, inplace=True)
    df['open_time'] = pd.to_datetime(df['open_time'])
    df.set_index('open_time', inplace=True)
    df['usdt'] = 1

    session.close()

    return df

def create_rolling_windows(df, window_size=720, step_size=24):
    """
    Tạo dữ liệu rolling windows với yield để tiết kiệm bộ nhớ.
    Parameters:
    - df: DataFrame gốc chứa dữ liệu
    - window_size: Kích thước cửa sổ (720 rows)
    - step_size: Bước nhảy (24 rows)
    Yield:
    - DataFrame với dữ liệu rolling window ở mỗi bước
    """
    total_rows = len(df)
    
    for end_idx in range(window_size, total_rows + 1, step_size):
        yield df.iloc[:end_idx]

def optimize_portfolio(df_filtered, target_return=0.2, min_weight=0.05):
    mu = expected_returns.mean_historical_return(df_filtered)
    S = CovarianceShrinkage(df_filtered).ledoit_wolf()

    # 1. Portfolio có tỷ lệ Sharpe cao nhất
    ef_sharpe = EfficientFrontier(mu, S)
    ef_sharpe.add_constraint(lambda w: w >= min_weight)  # Ràng buộc tối thiểu
    ef_sharpe.max_sharpe()
    weights_sharpe = ef_sharpe.clean_weights()
    performance_sharpe = ef_sharpe.portfolio_performance(verbose=False)

    # 2. Portfolio tối ưu
    ef_optimal = EfficientFrontier(mu, S)
    ef_optimal.add_constraint(lambda w: w >= min_weight)  # Ràng buộc tối thiểu
    ef_optimal.efficient_return(target_return=target_return)
    weights_optimal = ef_optimal.clean_weights()
    performance_optimal = ef_optimal.portfolio_performance(verbose=False)

    return {
        "sharpe": (weights_sharpe, performance_sharpe),
        "optimal": (weights_optimal, performance_optimal)
    }

if __name__ == "__main__":

    start_date = "2025-01-01"
    min_weight = 0.05
    target_return = 0.2

    df = fetch_ticker_data(start_date)

    rolling_results = []
    for window_df in create_rolling_windows(df):    
        results = optimize_portfolio(window_df, target_return, min_weight)
        print(results)
