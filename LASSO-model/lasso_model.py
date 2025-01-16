import time
import numpy as np
import pandas as pd
import config 
from sqlalchemy import Column, Float, String, DateTime, Integer, Table, MetaData, text
from sqlalchemy.orm import declarative_base
from sqlalchemy.orm import sessionmaker
from sklearn.linear_model import LassoCV
from sklearn.metrics import r2_score
from typing import Tuple
from datetime import datetime

# --- Database Configuration ---
Base = declarative_base()
engine = config.create_database_engine()
Session = sessionmaker(bind=engine)
session = Session()

class LassoOrders(Base):
    __tablename__ = 'lasso_orders'
    time = Column(DateTime, primary_key=True)
    order_symbol = Column(String(20))
    prediction = Column(Float)
    confidence = Column(Float)
    drawdown = Column(Float)
    hold_time = Column(Float)

Base.metadata.create_all(engine)

# --- Utility Functions ---
def fetch_data_from_db(symbol: str, limit: int = 1440) -> pd.DataFrame:
    """Fetch data for a given symbol from the database."""
    table_name = symbol.replace("-", "_").lower()

    query = text(f"SELECT * FROM {table_name} ORDER BY time ASC")
    result = session.execute(query)
    rows = result.fetchall()
    df = pd.DataFrame.from_records(rows, columns=result.keys())
    return df

def prepare_lagged_features(data: pd.DataFrame, lags: int = 3) -> Tuple[np.ndarray, np.ndarray]:
    """Prepare lagged features for LASSO model."""
    features = [data['close'].shift(i) for i in range(1, lags + 1)]
    features = pd.concat(features, axis=1).dropna()
    features.columns = [f'close_lag{i}' for i in range(1, lags + 1)]
    X = features.values
    y = data['close'][lags:].values
    return X, y

def train_lasso(X: np.ndarray, y: np.ndarray) -> Tuple[LassoCV, float]:
    """Train LASSO model and calculate R^2 score."""
    model = LassoCV(cv=5, random_state=42).fit(X, y)
    r2 = r2_score(y, model.predict(X))
    return model, r2

def save_order(symbol: str, prediction: float, confidence: float):
    """Save the best symbol and its prediction to the database."""
    current_time = pd.Timestamp.utcnow()

    order = LassoOrders(
        time=current_time,
        order_symbol=symbol,
        prediction=prediction,
        confidence=confidence,
        drawdown=0.0,  # Placeholder for now
        hold_time=0.0   # Placeholder for now
    )
    session.add(order)
    session.commit()

# --- Main Execution ---
def main(symbols):
    """Main loop to fetch data, train models, and save the best result."""
    last_run_time = None

    while True:
        # Lấy thời gian hiện tại
        current_time = time.time()

        # Nếu đây là lần chạy đầu tiên hoặc đã đủ 1 phút từ lần chạy trước
        if last_run_time is None or (current_time - last_run_time) >= 60:
            last_run_time = current_time

            # Dictionary to store results for each symbol
            results = {}

            # Loop through each symbol to fetch data and train LASSO model
            for symbol in symbols:
                data = fetch_data_from_db(symbol)
                if len(data) < 10:
                    print(f"Skipping {symbol} due to insufficient data ({len(data)} records).")
                    continue

                # Prepare lagged features
                X, y = prepare_lagged_features(data)
                if len(X) == 0:
                    print(f"Skipping {symbol} due to insufficient lagged data.")
                    continue

                # Train LASSO model and get prediction and R^2 score
                model, r2 = train_lasso(X, y)
                next_prediction = model.predict(X[-1].reshape(1, -1))[0]
                results[symbol] = {
                    'prediction': next_prediction,
                    'confidence': r2
                }

            # If no valid results, skip this iteration
            if not results:
                print("No valid results to save. Retrying...")
                continue

            # Find the best symbol based on confidence (R^2 score)
            best_symbol = max(results, key=lambda s: results[s]['confidence'])
            best_result = results[best_symbol]

            # Save the best symbol and its details to the database
            save_order(
                symbol=best_symbol,
                prediction=best_result['prediction'],
                confidence=best_result['confidence']
            )

            # Log the result
            print(
                f"Best symbol: {best_symbol}, "
                f"Prediction: {best_result['prediction']:.4f}, "
                f"R^2: {best_result['confidence']:.4f} "
                f" at {datetime.now()}"
            )

        # Chờ đến phút tiếp theo
        time.sleep(max(0, 60 - (time.time() % 60)))

if __name__ == "__main__":
    symbols = ["BTC-USDT", "ETH-USDT", "BNB-USDT", "XRP-USDT", "SOL-USDT"]
    main(symbols)