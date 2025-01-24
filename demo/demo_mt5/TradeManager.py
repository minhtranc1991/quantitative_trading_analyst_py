import pandas as pd
import MetaTrader5 as mt5

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