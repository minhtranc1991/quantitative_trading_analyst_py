import pandas as pd
import yfinance as yf
import concurrent.futures
import matplotlib.pyplot as plt
from datetime import date, timedelta
from pypfopt import EfficientFrontier, expected_returns, objective_functions
from pypfopt.risk_models import CovarianceShrinkage

def download_data(basket, start_date, end_date):
    data = {}
    for ticker in basket:
        df_ticker = yf.download(ticker, start=start_date, end=end_date)['Close']
        data[ticker] = df_ticker
    df = pd.concat(data, axis=1).ffill().fillna(0)
    if df.isnull().values.any():
        raise ValueError("Data contains NaN values even after filling!")
    return df

def filter_top_assets(df, max_assets, criterion="sharpe", required_assets=None):
    # Tính lợi suất kỳ vọng
    mu = expected_returns.mean_historical_return(df)
    # Tính độ lệch chuẩn từ lợi suất hàng ngày
    daily_returns = df.pct_change().dropna()
    if daily_returns.isna().any().any():
        raise ValueError("The data after calculation contains NaN. Please check the input data.")
    
    std_devs = daily_returns.std()

    if criterion == "sharpe":
        sharpe_ratios = mu / std_devs  # Tính Sharpe Ratio
        if isinstance(sharpe_ratios.index, pd.MultiIndex):
            sharpe_ratios.index = sharpe_ratios.index.map(lambda x: x[0])
        top_assets = sharpe_ratios.nlargest(max_assets).index.tolist()  # Lấy top tài sản theo Sharpe Ratio
    elif criterion == "volatility":
        std_devs.index = std_devs.index.map(lambda x: x[0])
        top_assets = std_devs.nsmallest(max_assets).index.tolist()  # Lấy top tài sản có độ biến động thấp nhất
    else:
        raise ValueError("Criterion not supported! Use 'sharpe' or 'volatility'.")

    # Đảm bảo các tài sản bắt buộc luôn có mặt
    if required_assets is not None:
        # Loại bỏ các tài sản trong required_assets khỏi top_assets
        top_assets = [asset for asset in top_assets if asset not in required_assets]
        # Thêm các tài sản bắt buộc vào đầu danh sách
        top_assets = required_assets + top_assets
        # Giữ đúng số lượng tài sản tối đa
        top_assets = top_assets[:max_assets]

    print(top_assets)  # In danh sách ticker
    return df[top_assets]

def optimize_portfolio(df_filtered, target_return=0.2, min_weight=0.05): 
    mu = expected_returns.mean_historical_return(df_filtered)
    S = CovarianceShrinkage(df_filtered).ledoit_wolf()

    # 1. Portfolio có tỷ lệ Sharpe cao nhất
    ef_sharpe = EfficientFrontier(mu, S)
    ef_sharpe.add_constraint(lambda w: w >= min_weight)  # Ràng buộc tối thiểu
    ef_sharpe.max_sharpe()
    weights_sharpe = ef_sharpe.clean_weights()
    performance_sharpe = ef_sharpe.portfolio_performance(verbose=False)

    # 2. Portfolio có độ biến động thấp nhất
    ef_volatility = EfficientFrontier(mu, S)
    ef_volatility.add_constraint(lambda w: w >= min_weight)  # Ràng buộc tối thiểu
    ef_volatility.min_volatility()
    weights_volatility = ef_volatility.clean_weights()
    performance_volatility = ef_volatility.portfolio_performance(verbose=False)

    # 3. Portfolio tối ưu (giữa rủi ro và lợi suất)
    ef_optimal = EfficientFrontier(mu, S)
    ef_optimal.add_constraint(lambda w: w >= min_weight)  # Ràng buộc tối thiểu
    ef_optimal.efficient_return(target_return=target_return)
    weights_optimal = ef_optimal.clean_weights()
    performance_optimal = ef_optimal.portfolio_performance(verbose=False)

    return {
        "sharpe": (weights_sharpe, performance_sharpe),
        "volatility": (weights_volatility, performance_volatility),
        "optimal": (weights_optimal, performance_optimal),
    }

def plot_portfolio(weights, title):
    filtered_weights = {key: weight for key, weight in weights.items() if weight > 0}
    assets = list(filtered_weights.keys())
    weights = list(filtered_weights.values())

    plt.figure(figsize=(8, 8))
    plt.pie(weights, labels=assets, autopct='%1.1f%%', startangle=140, colors=plt.cm.tab20.colors)
    plt.title(title)
    plt.show()

def main(basket, start_date, end_date, max_assets, min_weight, target_return, criterion, required_assets=None):
    # Download data
    df = download_data(basket, start_date, end_date)

    # Filter top assets
    df = filter_top_assets(df, max_assets, criterion, required_assets)

    # Optimize portfolio
    results = optimize_portfolio(df, target_return, min_weight)

    # Create Portfolio Comparison DataFrame
    portfolio_data = {
        "Portfolio Type": ["Max Sharpe", "Min Volatility", "Optimal Portfolio"],
        "Return": [
            results["sharpe"][1][0],
            results["volatility"][1][0],
            results["optimal"][1][0],
        ],
        "Volatility": [
            results["sharpe"][1][1],
            results["volatility"][1][1],
            results["optimal"][1][1],
        ],
        "Sharpe Ratio": [
            results["sharpe"][1][2],
            results["volatility"][1][2],
            results["optimal"][1][2],
        ],
    }
    portfolio_df = pd.DataFrame(portfolio_data)

    for column in ["Return", "Volatility"]:
        portfolio_df[column] = portfolio_df[column].apply(lambda x: f"{x * 100:.2f}%")

    print("Portfolio Comparison:\n", portfolio_df)

    # Display results for each portfolio type
    for portfolio_type, (weights, performance) in results.items():
        weight = pd.DataFrame([
          {'Token': 'GOLD' if k[0] == 'GC=F' else k[0].replace('-USD', ''), 'Allocation': f"{v*100:.2f}%"}
          for k, v in weights.items()
        ])

        print(f"\n{portfolio_type.capitalize()} Portfolio:")
        print("Weights:", weight)
        print("Performance: Return = {:.2f}%, Volatility = {:.2f}%, Sharpe Ratio = {:.2f}".format(
            performance[0] * 100, performance[1] * 100, performance[2]
        ))
        plot_portfolio(weights, f"{portfolio_type.capitalize()} Portfolio Allocation")

# Example Usage
if __name__ == "__main__":
    basket = ["GC=F", "BTC-USD", "ETH-USD", "ONUS-USD", "XRP-USD", "SOL-USD", "BNB-USD", "DOGE-USD", "ADA-USD", "TRX-USD",
          "AVAX-USD", "LINK-USD", "XLM-USD", "HBAR-USD", "SUI20947-USD", "TON11419-USD", "SHIB-USD", "DOT-USD", "LTC-USD", "BCH-USD",
          "VET-USD", "UNI-USD", "HYPE-USD", "ETC-USD", "PEPE24478-USD", "NEAR-USD", "AAVE-USD", "APT-USD", "ICP-USD", "ONDO-USD"]
    start_date = date.today() + timedelta(days=-364)
    #start_date = "2021-11-10"
    end_date = date.today().strftime("%Y-%m-%d")
    max_assets = 5
    min_weight = 0.05 
    target_return = 0.2
    criterion = "sharpe"  # "sharpe" or "volatility"
    required_assets = ['BTC-USD', 'GC=F', 'ONUS-USD']

    main(basket, start_date, end_date, max_assets, min_weight, target_return, criterion, required_assets)
