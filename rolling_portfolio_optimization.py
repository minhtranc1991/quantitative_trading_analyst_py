import config
import pandas as pd
from sqlalchemy import Table, MetaData, select
from sqlalchemy.orm import sessionmaker

def fetch_ticker_data(start_date="2020-09-01"):
    engine = config.create_database_engine()
    Session = sessionmaker(bind=engine)
    session = Session()

    # Khởi tạo metadata và bảng
    db_metadata = MetaData()
    tickers = Table('tickers', db_metadata, autoload_with=engine, schema='quant_trading')

    # Tạo truy vấn
    query = select(tickers.c.ticker).where(
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
    final_df = pd.concat(data_frames, axis=1).fillna(0).reset_index()

    session.close()

    return final_df

if __name__ == "__main__":
    df = fetch_ticker_data()
    
