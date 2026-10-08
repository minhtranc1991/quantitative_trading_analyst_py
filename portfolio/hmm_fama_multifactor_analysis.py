import polars as pl
import pandas as pd
import yfinance as yf
import statsmodels.api as sm
import matplotlib.pyplot as plt
from hmmlearn.hmm import GaussianHMM

stocks = ["SPY", "BAC", "AES", "DCOM"]

data = {stock: yf.download(stock, start="2003-01-01", end="2023-12-31") for stock in stocks}

dataframes = {
    stock: (
        pl.from_pandas(data[stock].reset_index())
        .rename({
            f"('Date', '')": "Date",
            f"('Adj Close', '{stock}')": "Adj Close",
            f"('Close', '{stock}')": "Close",
            f"('High', '{stock}')": "High",
            f"('Low', '{stock}')": "Low",
            f"('Open', '{stock}')": "Open",
            f"('Volume', '{stock}')": "Volume",
        })
    )
    for stock in stocks
}

fama_french_url = "https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/ftp/F-F_Research_Data_5_Factors_2x3_daily_CSV.zip"

factors = pd.read_csv(fama_french_url,
                      compression='zip',
                      skiprows=3,
                      header=0)

factors.rename(columns={factors.columns[0]: "Date"}, inplace=True)
factors["Date"] = pd.to_datetime(factors["Date"], format="%Y%m%d", errors="coerce")

factors = pl.DataFrame(factors)

factors = factors.with_columns([
    pl.col("Mkt-RF").cast(pl.Float64),
    pl.col("SMB").cast(pl.Float64),
    pl.col("HML").cast(pl.Float64),
    pl.col("RMW").cast(pl.Float64),
    pl.col("CMA").cast(pl.Float64),
    pl.col("RF").cast(pl.Float64),
])

# Hàm tính log-returns
def calculate_log_returns(df):
    df = df.with_columns(
        (pl.col("Close") / pl.col("Close").shift(1)).log().alias("Log_Returns")
    )
    return df.drop_nulls()

# Áp dụng cho từng cổ phiếu
processed_data = {stock: calculate_log_returns(dataframes[stock]) for stock in stocks}

def apply_hmm(log_returns, n_states=3):
    hmm = GaussianHMM(n_components=n_states, covariance_type="diag", n_iter=1000)
    hmm.fit(log_returns.to_pandas()[["Log_Returns"]])
    states = hmm.predict(log_returns.to_pandas()[["Log_Returns"]])
    return log_returns.with_columns(pl.Series("State", states))

# Xác định trạng thái cho từng cổ phiếu
state_data = {stock: apply_hmm(processed_data[stock], n_states=3) for stock in stocks}

# Hàm phân tích Fama-French
def fama_french_analysis(df, factors_df):
    merged = df.join(factors_df, on="Date")
    y = merged["Log_Returns"].to_numpy()
    X = merged[["Mkt-RF", "SMB", "HML"]].to_numpy()
    X = sm.add_constant(X)
    model = sm.OLS(y, X).fit()
    return model.summary()

# Phân tích cho từng trạng thái của SPY
spy_states = state_data["SPY"]
factor_analysis = {state: fama_french_analysis(state_data[stock], factors) for state,
                   stock in enumerate(stocks)}

for stock, analysis in factor_analysis.items():
    print(f"Stock analysis: {stocks[stock]}")
    print(analysis)
    print("-" * 50)

for stock, df in state_data.items():
    plt.figure(figsize=(10, 6))
    plt.plot(df["Date"], df["State"])
    plt.title(f"Market States for {stock}")
    plt.show()
