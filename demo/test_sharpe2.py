import sharpe_ratio
import math

# Test case 1: Danh sách rỗng
print(sharpe_ratio.calculate_sharpe_ratio([]))
    
# Test case 2: Độ lệch chuẩn bằng 0
print(sharpe_ratio.calculate_sharpe_ratio([0.02, 0.02, 0.02]))
    
# Test case 3: Tính toán chính xác
returns = [0.01, 0.02, -0.01]
expected = (0.0066667 / 0.015275) * math.sqrt(252)
print(abs(sharpe_ratio.calculate_sharpe_ratio(returns) - expected))