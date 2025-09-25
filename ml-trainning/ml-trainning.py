import pandas as pd

# Load data
df = pd.read_csv("ml-trainning/spx.csv")

# Basic info
print("=== Dataset Info ===")
print(f"Number of rows: {len(df)}")
print(f"Columns: {list(df.columns)}")

# Check date range (assuming 'Date' column exists)
if 'Date' in df.columns:
    df['Date'] = pd.to_datetime(df['Date'])
    print(f"Date Range: {df['Date'].min()} to {df['Date'].max()}")
else:
    print("No Date column found.")

# Check missing data
print("\n=== Missing Data ===")
print(df.isnull().sum())

# Basic descriptive statistics
print("\n=== Descriptive Statistics ===")
print(df.describe(include='all'))

# Additional: Min/Max values for price columns
numeric_cols = df.select_dtypes(include='number').columns
for col in numeric_cols:
    print(f"\nColumn: {col}")
    print(f"  Min: {df[col].min()}")
    print(f"  Max: {df[col].max()}")
    print(f"  Mean: {df[col].mean()}")
    print(f"  Std: {df[col].std()}")
