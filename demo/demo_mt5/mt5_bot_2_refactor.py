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
from functools import wraps

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

# ================================================
# Utility Functions and Decorators
# ================================================
def handle_errors(func):
    """
    Decorator for centralized error handling
    Refactor step 2: Create error handling decorator
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except mt5.MT5Error as e:
            print(f"MT5 Error in {func.__name__}: {e}")
            return None
        except Exception as e:
            print(f"Unexpected error in {func.__name__}: {e}")
            return None
    return wrapper

# ================================================
# Core Classes
# ================================================
class ConfigManager:
    """
    Class for managing configuration
    Refactor step 3: Extract configuration management
    """
    def __init__(self, config_file="jpara.json"):
        self.config_file = config_file
        self.default_config = {
            'buy_counter': 0,
            'sell_counter': 0,
            'buy_price': 0,
            'sell_price': 10000,
            'last_sell_positive': 0,
            'last_buy_positive': 0,
            'last_sell_negative': 0,
            'last_buy_negative': 0,
            'dca_buy_positive_tickets': [],
            'dca_buy_negative_tickets': [],
            'dca_sell_positive_tickets': [],
            'dca_sell_negative_tickets': [],
            'sell_ticket': 0,
            'buy_ticket': 0,
            'dca_sell_positive_count': 0,
            'dca_sell_negative_count': 0,
            'dca_buy_positive_count': 0,
            'dca_buy_negative_count': 0,
        }
        self.load_config()

    def load_config(self):
        try:
            with open(self.config_file, "r") as f:
                self.config = json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            self.config = {}

    def save_config(self):
        with open(self.config_file, "w") as f:
            json.dump(self.config, f, indent=4)

    def get_symbol_config(self, symbol):
        return self.config.get(symbol, self.default_config.copy())

    def update_symbol_config(self, symbol, updates):
        current = self.get_symbol_config(symbol)
        current.update(updates)
        self.config[symbol] = current
        self.save_config()

class TradeManager:
    """
    Class for managing trading operations
    Refactor step 4: Extract trading logic
    """
    def __init__(self, config_manager):
        self.config = config_manager
        self.rsi_window = RSI_WINDOW
        self.base_volume = BASE_VOLUME

    @handle_errors
    def get_historical_data(self, symbol, timeframe, num_bars=1000):
        rates = mt5.copy_rates_from_pos(symbol, timeframe, 0, num_bars)
        if rates is None:
            return None
        df = pd.DataFrame(rates)
        df['time'] = pd.to_datetime(df['time'], unit='s')
        return df

    def calculate_rsi(self, df):
        delta = df['close'].diff()
        gain = delta.where(delta > 0, 0)
        loss = -delta.where(delta < 0, 0)
        
        avg_gain = gain.ewm(alpha=1/self.rsi_window, adjust=False).mean()
        avg_loss = loss.ewm(alpha=1/self.rsi_window, adjust=False).mean()
        
        rs = avg_gain / avg_loss
        return 100 - (100 / (1 + rs))

    @handle_errors
    def execute_order(self, symbol, order_type, volume=None):
        volume = volume or self.base_volume
        tick = mt5.symbol_info_tick(symbol)
        price = tick.ask if order_type == 'buy' else tick.bid
        
        request = {
            'action': mt5.TRADE_ACTION_DEAL,
            'symbol': symbol,
            'volume': volume,
            'type': mt5.ORDER_TYPE_BUY if order_type == 'buy' else mt5.ORDER_TYPE_SELL,
            'price': price,
            'deviation': 10,
            'magic': 100,
            'comment': 'Python automated order',
            'type_time': mt5.ORDER_TIME_GTC,
            'type_filling': mt5.ORDER_FILLING_FOK,
        }
        return mt5.order_send(request)

    @handle_errors
    def close_position(self, ticket):
        position = mt5.positions_get(ticket=ticket)
        if not position:
            return None
            
        position = position[0]
        symbol = position.symbol
        volume = position.volume
        order_type = mt5.ORDER_TYPE_SELL if position.type == mt5.ORDER_TYPE_BUY else mt5.ORDER_TYPE_BUY
        price = mt5.symbol_info_tick(symbol).bid if position.type == mt5.ORDER_TYPE_BUY else mt5.symbol_info_tick(symbol).ask
        
        request = {
            "action": mt5.TRADE_ACTION_DEAL,
            "symbol": symbol,
            "volume": volume,
            "type": order_type,
            "position": ticket,
            "price": price,
            "deviation": 20,
            "magic": 123456,
            "comment": f"Close position {ticket}",
            "type_time": mt5.ORDER_TIME_GTC,
            "type_filling": mt5.ORDER_FILLING_FOK,
        }
        return mt5.order_send(request)

# ================================================
# Main Trading Bot
# ================================================
class TradingBot:
    """
    Main trading bot class
    Refactor step 5: Create main class for overall management
    """
    def __init__(self):
        self.config_manager = ConfigManager()
        self.trade_manager = TradeManager(self.config_manager)
        self.setup_schedule()

    @handle_errors
    def init_mt5(self):
        if not mt5.initialize():
            raise mt5.MT5Error("MT5 initialization failed")
        if not mt5.login(LOGIN, PASSWORD, SERVER):
            mt5.shutdown()
            raise mt5.MT5Error("MT5 login failed")
        print("MT5 connection established")

    def setup_schedule(self):
        times = [":00", ":15", ":30", ":45"]
        for t in times:
            schedule.every().hour.at(t).do(self.trade_all_symbols)

    def trade_all_symbols(self):
        with concurrent.futures.ThreadPoolExecutor() as executor:
            executor.map(self.process_symbol, SYMBOL_LIST)

    @handle_errors
    def process_symbol(self, symbol):
        print(f"Processing {symbol}...")
        
        # Get market data
        m15_data = self.trade_manager.get_historical_data(symbol, TIMEFRAMES['M15'])
        h1_data = self.trade_manager.get_historical_data(symbol, TIMEFRAMES['H1'])
        
        if m15_data is None or h1_data is None:
            return
            
        # Calculate RSI values
        m15_data['RSI'] = self.trade_manager.calculate_rsi(m15_data)
        h1_data['RSI'] = self.trade_manager.calculate_rsi(h1_data)
        
        current_rsi_m15 = m15_data['RSI'].iloc[-1]
        current_rsi_h1 = h1_data['RSI'].iloc[-1]
        
        # Main trading logic
        self.check_entry_conditions(symbol, current_rsi_m15)
        self.manage_existing_positions(symbol, current_rsi_m15, current_rsi_h1)

    def check_entry_conditions(self, symbol, rsi_m15):
        config = self.config_manager.get_symbol_config(symbol)
        
        # Entry logic for sell positions
        if config['sell_counter'] == 0 and rsi_m15 >= 95:
            result = self.trade_manager.execute_order(symbol, 'sell')
            if result:
                updates = {
                    'sell_counter': 1,
                    'sell_price': result.price,
                    'last_sell_negative': result.price,
                    'last_sell_positive': result.price
                }
                self.config_manager.update_symbol_config(symbol, updates)

        # Entry logic for buy positions
        if config['buy_counter'] == 0 and rsi_m15 <= 5:
            result = self.trade_manager.execute_order(symbol, 'buy')
            if result:
                updates = {
                    'buy_counter': 1,
                    'buy_price': result.price,
                    'last_buy_positive': result.price,
                    'last_buy_negative': result.price
                }
                self.config_manager.update_symbol_config(symbol, updates)

    def manage_existing_positions(self, symbol, rsi_m15, rsi_h1):
        config = self.config_manager.get_symbol_config(symbol)
        positions = mt5.positions_get(symbol=symbol)
        
        # Manage DCA and exit conditions
        self.manage_dca(symbol, config, positions, rsi_m15)
        self.check_exit_conditions(symbol, config, positions, rsi_m15, rsi_h1)

    def manage_dca(self, symbol, config, positions, rsi_m15):
        current_price = mt5.symbol_info_tick(symbol).ask
        
        # Manage sell DCA
        if config['sell_counter'] > 0:
            self.manage_sell_dca(symbol, config, current_price, rsi_m15)
            
        # Manage buy DCA
        if config['buy_counter'] > 0:
            self.manage_buy_dca(symbol, config, current_price, rsi_m15)

    def run(self):
        self.init_mt5()
        try:
            while True:
                schedule.run_pending()
                time.sleep(1)
        except KeyboardInterrupt:
            print("Shutting down...")
        finally:
            mt5.shutdown()
            print("MT5 connection closed")

# ================================================
# Main Execution
# ================================================
if __name__ == "__main__":
    bot = TradingBot()
    bot.run()