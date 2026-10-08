import pandas as pd
import numpy as np

# Load the data
df = pd.read_csv("ml-trainning/spx.csv")

# Assume columns include 'Date' and 'close'; adjust if necessary
df['Date'] = pd.to_datetime(df['Date'])
df = df.sort_values('Date').reset_index(drop=True)
df['return'] = df['close'].pct_change()
df = df.dropna()  # Drop the first row with NaN return

# Split into train and test sets (80/20 split)
split_index = int(0.8 * len(df))
train = df.iloc[:split_index].copy()
test = df.iloc[split_index:].copy()

# Function to compute metrics: Sharpe, CAGR, Calmar
# Assuming risk-free rate = 0, annualized using 252 trading days
def compute_metrics(returns):
    if len(returns) == 0:
        return np.nan, np.nan, np.nan
    
    # Sharpe Ratio (annualized)
    mean_ret = returns.mean()
    std_ret = returns.std()
    sharpe = mean_ret / std_ret * np.sqrt(252) if std_ret != 0 else np.nan
    
    # CAGR
    cum_prod = (1 + returns).cumprod()
    total_return = cum_prod.iloc[-1] - 1
    num_years = len(returns) / 252
    cagr = (1 + total_return) ** (1 / num_years) - 1 if num_years > 0 else np.nan
    
    # Calmar Ratio
    peak = cum_prod.cummax()
    drawdown = (cum_prod - peak) / peak
    max_drawdown = drawdown.min()
    calmar = cagr / -max_drawdown if max_drawdown != 0 else np.inf
    
    return sharpe, cagr, calmar

# Benchmark 1: Buy-and-Hold (BAH)
bah_train_sharpe, bah_train_cagr, bah_train_calmar = compute_metrics(train['return'])
bah_test_sharpe, bah_test_cagr, bah_test_calmar = compute_metrics(test['return'])

# Benchmark 2: Naive Model
# Predict tomorrow's return = today's return
# Strategy: Buy if predicted > 0, else hold cash (0 return)
def naive_strategy_returns(df_returns):
    # Shift returns by 1 to get prediction (pred_t = return_{t-1})
    pred = df_returns.shift(1)
    # Strategy return: actual return if pred > 0, else 0
    strategy_ret = np.where(pred > 0, df_returns, 0)
    strategy_ret = pd.Series(strategy_ret, index=df_returns.index).dropna()
    return strategy_ret

naive_train_returns = naive_strategy_returns(train['return'])
naive_test_returns = naive_strategy_returns(test['return'])

naive_train_sharpe, naive_train_cagr, naive_train_calmar = compute_metrics(naive_train_returns)
naive_test_sharpe, naive_test_cagr, naive_test_calmar = compute_metrics(naive_test_returns)

# Print results
print("Buy-and-Hold (BAH) Metrics:")
print("Train - Sharpe: {:.4f}, CAGR: {:.4f}, Calmar: {:.4f}".format(bah_train_sharpe, bah_train_cagr, bah_train_calmar))
print("Test - Sharpe: {:.4f}, CAGR: {:.4f}, Calmar: {:.4f}".format(bah_test_sharpe, bah_test_cagr, bah_test_calmar))

print("\nNaive Model Metrics:")
print("Train - Sharpe: {:.4f}, CAGR: {:.4f}, Calmar: {:.4f}".format(naive_train_sharpe, naive_train_cagr, naive_train_calmar))
print("Test - Sharpe: {:.4f}, CAGR: {:.4f}, Calmar: {:.4f}".format(naive_test_sharpe, naive_test_cagr, naive_test_calmar))