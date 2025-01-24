"""
REFACTOR STEPS:
1. Separate configuration and constants
2. Create a centralized error handling decorator
3. Extract configuration management into a separate class
4. Extract trading logic into TradeManager class
5. Create main TradingBot class for overall management
6. Optimize data fetching and RSI calculation
7. Add multi-threading for symbol processing
8. Improve logging and error handling
9. Separate entry and exit conditions logic
10. Ensure proper connection management
"""

import MetaTrader5 as mt5
import pandas as pd
from datetime import datetime
import time
import concurrent.futures
import json
import schedule
import TradingBot
from functools import wraps

# ================================================
# Main Execution
# ================================================
if __name__ == "__main__":

    # ================================================
    # Configuration and Constants
    # ================================================
    LOGIN = '1604953453457274'
    PASSWORD = 'Chu.fxtm@181'
    SERVER = 'ForexTimeFXTM-Demo01'
    SYMBOL_LIST = ['EURUSD', 'USDCHF', 'USDCAD', 'AUDUSD', 'EURJPY', 'EURGBP', 
                'EURCHF', 'GBPCHF', 'GBPCAD', 'GBPAUD', 'GBPNZD', 'XAUUSD', 
                'Brent', 'Crude']
    TIMEFRAMES = {
        'M15': mt5.TIMEFRAME_M15,
        'H1': mt5.TIMEFRAME_H1
    }
    BASE_VOLUME = 0.01
    RSI_WINDOW = 3

    # Initialize and run bot
    bot = TradingBot(LOGIN, PASSWORD, SERVER, SYMBOL_LIST, TIMEFRAMES, BASE_VOLUME, RSI_WINDOW)
    bot.run()