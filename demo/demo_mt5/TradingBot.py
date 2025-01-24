import MetaTrader5 as mt5
import ConfigManager
import TradeManager
import schedule
import concurrent.futures
import time

# ================================================
# Main Trading Bot
# ================================================
class TradingBot:
    """
    Main trading bot class with configurable parameters
    """
    def __init__(self, login, password, server, symbol_list, timeframes, base_volume, rsi_window):
        """
        Initialize TradingBot with required parameters

        Args:
            login (str): MT5 account login
            password (str): MT5 account password
            server (str): MT5 server
            symbol_list (list): List of trading symbols
            timeframes (dict): Dictionary of timeframes
            base_volume (float): Base trading volume
            rsi_window (int): RSI calculation window
        """
        self.login = login
        self.password = password
        self.server = server
        self.symbol_list = symbol_list
        self.timeframes = timeframes
        self.base_volume = base_volume
        self.rsi_window = rsi_window

        # Initialize components
        self.config_manager = ConfigManager()
        self.trade_manager = TradeManager(self.config_manager, base_volume, rsi_window)
        self.setup_schedule()

    @handle_errors
    def init_mt5(self):
        """
        Initialize MT5 connection
        """
        if not mt5.initialize():
            raise mt5.MT5Error("MT5 initialization failed")
        if not mt5.login(self.login, self.password, self.server):
            mt5.shutdown()
            raise mt5.MT5Error("MT5 login failed")
        print("MT5 connection established")

    def setup_schedule(self):
        """
        Setup trading schedule
        """
        times = [":00", ":15", ":30", ":45"]
        for t in times:
            schedule.every().hour.at(t).do(self.trade_all_symbols)

    def trade_all_symbols(self):
        """
        Process all symbols using thread pool
        """
        with concurrent.futures.ThreadPoolExecutor() as executor:
            executor.map(self.process_symbol, self.symbol_list)

    @handle_errors
    def process_symbol(self, symbol):
        """
        Process trading logic for a single symbol

        Args:
            symbol (str): Symbol to process
        """
        print(f"Processing {symbol}...")
        
        # Get market data
        m15_data = self.trade_manager.get_historical_data(symbol, self.timeframes['M15'])
        h1_data = self.trade_manager.get_historical_data(symbol, self.timeframes['H1'])
        
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
        """
        Check and execute entry conditions for a symbol

        Args:
            symbol (str): Symbol to check
            rsi_m15 (float): Current RSI value on M15 timeframe
        """
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
        """
        Manage existing positions for a symbol

        Args:
            symbol (str): Symbol to manage
            rsi_m15 (float): Current RSI value on M15 timeframe
            rsi_h1 (float): Current RSI value on H1 timeframe
        """
        config = self.config_manager.get_symbol_config(symbol)
        positions = mt5.positions_get(symbol=symbol)
        
        # Manage DCA and exit conditions
        self.manage_dca(symbol, config, positions, rsi_m15)
        self.check_exit_conditions(symbol, config, positions, rsi_m15, rsi_h1)

    def run(self):
        """
        Main execution loop for the trading bot
        """
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