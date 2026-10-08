import json

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