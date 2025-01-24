from math import sqrt
from typing import List, Optional

def calculate_mean(returns: List[float]) -> float:
    """Tính trung bình (mean) của một danh sách lợi nhuận"""
    return sum(returns) / len(returns) if returns else 0.0

def calculate_std_dev(returns: List[float]) -> float:
    """Tính độ lệch chuẩn (standard deviation) của lợi nhuận"""
    if len(returns) < 2:
        return 0.0
    mean = calculate_mean(returns)
    squared_diffs = [(r - mean) ** 2 for r in returns]
    variance = sum(squared_diffs) / (len(returns) - 1)  # Sample variance
    return sqrt(variance)

def calculate_sharpe_ratio(
    portfolio_returns: List[float],
    risk_free_rate: float = 0.02,
    time_period: int = 252
) -> Optional[float]:
    """
    Tính Sharpe Ratio với risk-free rate cố định

    Args:
        portfolio_returns: Danh sách lợi nhuận theo kỳ (ví dụ: daily returns)
        risk_free_rate: Tỷ lệ phi rủi ro (mặc định 2%)
        time_period: Số kỳ trong năm để annualize (mặc định 252 ngày)

    Returns:
        Sharpe Ratio hoặc None nếu không tính được
    """
    if not portfolio_returns:
        return None
    
    # Tính excess returns
    excess_returns = [r - risk_free_rate for r in portfolio_returns]
    
    # Tính các thành phần
    mean_excess = calculate_mean(excess_returns)
    std_dev_excess = calculate_std_dev(excess_returns)
    
    if std_dev_excess == 0:
        return None
    
    # Annualize và tính Sharpe Ratio
    return (mean_excess / std_dev_excess) * sqrt(time_period)