import pandas as pd
import numpy as np

# Load data
df = pd.read_csv("ml-trainning/spx.csv")

# Ensure Date is datetime
df['Date'] = pd.to_datetime(df['Date'])
df = df.sort_values('Date')

# Assume column 'spx' is the index level
df['Return'] = df['spx'].pct_change()

# Compute cumulative return
df['Cumulative'] = (1 + df['Return']).cumprod()

# CAGR
start_value = df['spx'].iloc[0]
end_value = df['spx'].iloc[-1]
n_years = (df['Date'].iloc[-1] - df['Date'].iloc[0]).days / 365.25
CAGR = (end_value / start_value) ** (1 / n_years) - 1

# Max Drawdown
roll_max = df['Cumulative'].cummax()
drawdown = df['Cumulative'] / roll_max - 1
max_drawdown = drawdown.min()

# Calmar Ratio
calmar_ratio = CAGR / abs(max_drawdown)

print("=== Performance Metrics ===")
print(f"CAGR: {CAGR:.4f}")
print(f"Max Drawdown: {max_drawdown:.4f}")
print(f"Calmar Ratio: {calmar_ratio:.4f}")
